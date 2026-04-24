"""Browser-based GUI for the VEX monitoring agent."""

from __future__ import annotations

import argparse
import html
import logging
import threading
import time
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server
from typing import Callable

from config import load_settings
from main import run_ai_rankings_cycle, run_competition_cycle, run_full_cycle, run_media_cycle
from storage.db import (
    build_dashboard_view,
    db_session,
    get_available_teams,
    init_db,
    utc_now,
)
from utils.logging import configure_logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

LOGGER = logging.getLogger(__name__)
REFRESH_STATE: dict[str, object] = {"status": "idle", "message": "", "last_started_at": "", "last_completed_at": ""}
REFRESH_LOCK = threading.Lock()
MEDIA_STATE: dict[str, object] = {
    "status": "idle",
    "message": "",
    "last_started_at": "",
    "last_completed_at": "",
    "last_new_items": 0,
    "last_error": "",
    "last_partial_count": 0,
}
MEDIA_LOCK = threading.Lock()
MEDIA_THREAD: threading.Thread | None = None
COMPETITION_THREAD: threading.Thread | None = None


def template_environment() -> Environment:
    """Build the Jinja2 environment for the GUI."""
    template_dir = Path(__file__).resolve().parent / "templates"
    return Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(enabled_extensions=("html",)),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def status_banner(view: dict[str, object]) -> dict[str, str]:
    """Build a compact status summary for the header."""
    snapshot = view.get("latest_snapshot") or {}
    power_row = view.get("team_power") or {}
    dashboard_health = view.get("dashboard_health") or {}
    if not snapshot:
        return {"headline": "No snapshot yet", "subtext": "Run the collectors to populate the dashboard."}
    fetched_at = snapshot.get("fetched_at", "unknown")
    power_text = f" · Power #{power_row.get('power_rank')}" if power_row else ""
    ai_rankings = view.get("ai_rankings") or {}
    ai_text = f" · AI {ai_rankings.get('confidence', {}).get('level', '').title()}" if ai_rankings else ""
    health_text = f" · Health {str(dashboard_health.get('status') or 'unknown').title()}"
    return {
        "headline": f"Team {snapshot.get('team_number')} - Rank #{snapshot.get('rank') or 'N/A'}{power_text}{ai_text}{health_text}",
        "subtext": f"Last official fetch: {fetched_at}",
    }


def _normalized_team_number(value: str | None) -> str | None:
    """Normalize a team-number query value."""
    if value in (None, ""):
        return None
    return str(value).strip().upper() or None


def _with_team_query(path: str, team_number: str | None) -> str:
    """Append the current team query to a route when present."""
    normalized = _normalized_team_number(team_number)
    if not normalized:
        return path
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}team={normalized}"


def _redirect_url(path: str, message: str, team_number: str | None) -> str:
    """Build a redirect URL preserving the selected team."""
    base = _with_team_query(path, team_number)
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}message={message}"


def view_context(
    active_tab: str,
    action_message: str = "",
    team_number: str | None = None,
    current_path: str = "/",
) -> dict[str, object]:
    """Load the current application view from SQLite."""
    settings = load_settings()
    with db_session(settings.db_path) as connection:
        init_db(connection)
        available_teams = get_available_teams(connection, settings.team_number, limit=250)
        requested_team = _normalized_team_number(team_number) or settings.team_number
        valid_teams = {str(item.get("team_number") or "").upper() for item in available_teams}
        selected_team = requested_team if requested_team in valid_teams else settings.team_number
        view = build_dashboard_view(connection, selected_team, settings)
    team_query = f"?team={selected_team}" if selected_team else ""
    view["settings"] = settings
    view["active_tab"] = active_tab
    view["selected_team_number"] = selected_team
    view["available_teams"] = view.get("available_teams") or available_teams
    view["team_query"] = team_query
    view["current_path"] = current_path
    view["nav_items"] = [
        ("dashboard", _with_team_query("/", selected_team), "Dashboard"),
        ("analysis", _with_team_query("/analysis", selected_team), "Analysis"),
        ("ai_rankings", _with_team_query("/ai-rankings", selected_team), "AI Rankings"),
        ("rankings", _with_team_query("/rankings", selected_team), "Rankings"),
        ("matches", _with_team_query("/matches", selected_team), "Matches"),
        ("media", _with_team_query("/media", selected_team), "Media"),
        ("history", _with_team_query("/history", selected_team), "History"),
        ("settings", _with_team_query("/settings", selected_team), "Settings"),
    ]
    view["status_banner"] = status_banner(view)
    view["action_message"] = action_message
    view["refresh_state"] = dict(REFRESH_STATE)
    view["media_state"] = dict(MEDIA_STATE)
    return view


def _sorted_threats(threats: list[dict[str, object]], sort_key: str, descending: bool) -> list[dict[str, object]]:
    """Return threat rows sorted by a supported key."""
    supported_keys = {
        "team_number",
        "official_rank",
        "power_rank",
        "skills_total",
        "opr",
        "threat_score",
        "official_pressure",
        "power_pressure",
        "skills_pressure",
        "scoring_pressure",
        "threat_level",
    }
    if sort_key not in supported_keys:
        sort_key = "threat_score"

    def _value(item: dict[str, object]) -> object:
        value = item.get(sort_key)
        if value is None:
            if sort_key == "team_number":
                return "ZZZZZZ"
            return float("-inf") if descending else float("inf")
        if sort_key == "threat_level":
            order = {"Critical": 4, "High": 3, "Moderate": 2, "Watch": 1}
            return order.get(str(value), 0)
        return value

    return sorted(threats, key=_value, reverse=descending)


def _next_threat_dir(current_sort: str, current_dir: str, requested_sort: str, default_dir: str) -> str:
    """Return the next direction for a sortable threat column link."""
    if current_sort == requested_sort:
        return "asc" if current_dir == "desc" else "desc"
    return default_dir


def render_template(template_name: str, context: dict[str, object]) -> bytes:
    """Render one template."""
    environment = template_environment()
    template = environment.get_template(template_name)
    return template.render(**context).encode("utf-8")


def html_response(
    start_response: Callable,
    body: bytes,
    status: str = "200 OK",
    headers: list[tuple[str, str]] | None = None,
) -> list[bytes]:
    """Write an HTML WSGI response."""
    response_headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
    if headers:
        response_headers.extend(headers)
    start_response(status, response_headers)
    return [body]


def redirect_response(start_response: Callable, location: str) -> list[bytes]:
    """Return an HTTP redirect response."""
    start_response("302 Found", [("Location", location), ("Content-Length", "0")])
    return [b""]


def _form_fields(environ: dict[str, object]) -> dict[str, str]:
    """Parse a small urlencoded POST body."""
    content_length = int(str(environ.get("CONTENT_LENGTH") or "0") or 0)
    body = environ["wsgi.input"].read(content_length) if content_length > 0 else b""
    parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
    return {key: values[0] for key, values in parsed.items()}


def create_app():
    """Create the WSGI application."""

    def app(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")
        query = parse_qs(environ.get("QUERY_STRING", ""))
        action_message = query.get("message", [""])[0]
        selected_team = _normalized_team_number(query.get("team", [""])[0])
        threat_sort = query.get("threat_sort", ["threat_score"])[0]
        threat_dir = query.get("threat_dir", ["desc"])[0]

        if method == "POST" and path == "/actions/run-now":
            settings = load_settings()
            try:
                run_full_cycle(settings)
                return redirect_response(start_response, _redirect_url("/", "Manual+refresh+completed", selected_team))
            except Exception as exc:
                return redirect_response(
                    start_response,
                    _redirect_url("/", f"Refresh+failed:+{str(exc).replace(' ', '+')}", selected_team),
                )

        if method == "POST" and path == "/actions/refresh-rankings":
            settings = load_settings()
            try:
                result = run_competition_cycle(settings)
                count = len(result.get("division_rankings", []))
                return redirect_response(
                    start_response,
                    _redirect_url("/rankings", f"Rankings+refresh+completed:+{count}+teams", selected_team),
                )
            except Exception as exc:
                return redirect_response(
                    start_response,
                    _redirect_url("/rankings", f"Rankings+refresh+failed:+{str(exc).replace(' ', '+')}", selected_team),
                )

        if method == "POST" and path == "/actions/refresh-ai-rankings":
            settings = load_settings()
            try:
                payload = run_ai_rankings_cycle(settings)
                return redirect_response(
                    start_response,
                    _redirect_url(
                        "/ai-rankings",
                        f"AI+rankings+refresh+completed:+{payload.get('confidence', {}).get('level', 'unknown')}",
                        selected_team,
                    ),
                )
            except Exception as exc:
                return redirect_response(
                    start_response,
                    _redirect_url("/ai-rankings", f"AI+rankings+refresh+failed:+{str(exc).replace(' ', '+')}", selected_team),
                )

        if method == "POST" and path == "/actions/refresh-media":
            settings = load_settings()
            started_at = utc_now()
            _set_media_state(
                status="running",
                message="Checking web, news, and enabled social sources...",
                last_started_at=started_at,
            )
            try:
                result = run_media_cycle(settings)
                new_count = len(result.get("new_media_items", []))
                source_failures = list(result.get("source_failures", []))
                partial = bool(result.get("partial"))
                _set_media_state(
                    status="partial" if partial else "success",
                    message=(
                        f"Media refresh complete with partial source coverage. {new_count} new items found."
                        if partial
                        else f"Media refresh complete. {new_count} new items found."
                    ),
                    last_completed_at=utc_now(),
                    last_new_items=new_count,
                    last_error="; ".join(source_failures[:3]),
                    last_partial_count=len(source_failures),
                )
                return redirect_response(
                    start_response,
                    _redirect_url("/media", f"Media+refresh+completed:+{new_count}+new+items", selected_team),
                )
            except Exception as exc:
                _set_media_state(
                    status="failed",
                    message=f"Media refresh failed. {exc}",
                    last_completed_at="",
                    last_error=str(exc),
                )
                return redirect_response(
                    start_response,
                    _redirect_url("/media", f"Media+refresh+failed:+{str(exc).replace(' ', '+')}", selected_team),
                )

        if path == "/":
            body = render_template("gui_dashboard.html.j2", view_context("dashboard", action_message, selected_team, "/"))
            return html_response(start_response, body)
        if path == "/rankings":
            context = view_context("rankings", action_message, selected_team, "/rankings")
            context["threat_sort"] = threat_sort
            context["threat_dir"] = threat_dir
            context["next_threat_dir"] = lambda requested_sort, default_dir="desc": _next_threat_dir(
                threat_sort,
                threat_dir,
                requested_sort,
                default_dir,
            )
            context["threat_list"] = _sorted_threats(
                list(context.get("threat_list") or []),
                threat_sort,
                threat_dir != "asc",
            )
            body = render_template("gui_rankings.html.j2", context)
            return html_response(start_response, body)
        if path == "/analysis":
            body = render_template("gui_analysis.html.j2", view_context("analysis", action_message, selected_team, "/analysis"))
            return html_response(start_response, body)
        if path == "/ai-rankings":
            context = view_context("ai_rankings", action_message, selected_team, "/ai-rankings")
            context["threat_sort"] = threat_sort
            context["threat_dir"] = threat_dir
            context["next_threat_dir"] = lambda requested_sort, default_dir="desc": _next_threat_dir(
                threat_sort,
                threat_dir,
                requested_sort,
                default_dir,
            )
            ai_rankings = context.get("ai_rankings") or {}
            if ai_rankings:
                ai_rankings["threat_rows"] = _sorted_threats(
                    list(ai_rankings.get("threat_rows") or []),
                    threat_sort,
                    threat_dir != "asc",
                )
                context["ai_rankings"] = ai_rankings
            body = render_template("gui_ai_rankings.html.j2", context)
            return html_response(start_response, body)
        if path == "/matches":
            body = render_template("gui_matches.html.j2", view_context("matches", action_message, selected_team, "/matches"))
            return html_response(start_response, body)
        if path == "/media":
            body = render_template("gui_media.html.j2", view_context("media", action_message, selected_team, "/media"))
            return html_response(start_response, body)
        if path == "/history":
            body = render_template("gui_history.html.j2", view_context("history", action_message, selected_team, "/history"))
            return html_response(start_response, body)
        if path == "/settings":
            body = render_template("gui_settings.html.j2", view_context("settings", action_message, selected_team, "/settings"))
            return html_response(start_response, body)

        body = f"<h1>404</h1><p>No route for {html.escape(path)}</p>".encode("utf-8")
        return html_response(start_response, body, status="404 Not Found")

    return app


def _set_refresh_state(**values: object) -> None:
    """Update the GUI launch-refresh state."""
    with REFRESH_LOCK:
        REFRESH_STATE.update(values)


def _set_media_state(**values: object) -> None:
    """Update the background media watcher state."""
    with MEDIA_LOCK:
        MEDIA_STATE.update(values)


def start_competition_watcher() -> None:
    """Run recurring competition and AI refresh inside the GUI process."""
    global COMPETITION_THREAD
    if COMPETITION_THREAD and COMPETITION_THREAD.is_alive():
        return

    def _runner() -> None:
        settings = load_settings()
        interval_seconds = max(300, int(settings.poll_interval_minutes) * 60)
        while True:
            started_at = utc_now()
            _set_refresh_state(
                status="running",
                message="Refreshing latest competition and AI rankings data in the background...",
                last_started_at=started_at,
            )
            try:
                run_competition_cycle(settings)
                _set_refresh_state(
                    status="success",
                    message=f"Background refresh complete. Auto-updating every {settings.poll_interval_minutes} minutes.",
                    last_completed_at=utc_now(),
                )
            except Exception as exc:
                LOGGER.warning("Background competition refresh failed", extra={"error": str(exc)})
                _set_refresh_state(
                    status="failed",
                    message=f"Background refresh failed; showing last stored data. {exc}",
                    last_completed_at="",
                )
            time.sleep(interval_seconds)

    COMPETITION_THREAD = threading.Thread(target=_runner, name="competition-watcher", daemon=True)
    COMPETITION_THREAD.start()


def start_media_watcher() -> None:
    """Run a dedicated background media watcher inside the GUI process."""
    global MEDIA_THREAD
    if MEDIA_THREAD and MEDIA_THREAD.is_alive():
        return

    settings = load_settings()
    if not settings.enable_background_media_watcher:
        _set_media_state(
            status="disabled",
            message="Background media watcher disabled.",
            last_error="",
        )
        return

    interval_seconds = max(300, int(settings.media_interval_minutes) * 60)

    def _runner() -> None:
        while True:
            started_at = utc_now()
            _set_media_state(
                status="running",
                message="Background media watcher checking web, official, and enabled social sources...",
                last_started_at=started_at,
            )
            try:
                result = run_media_cycle(settings)
                new_count = len(result.get("new_media_items", []))
                source_failures = list(result.get("source_failures", []))
                partial = bool(result.get("partial"))
                _set_media_state(
                    status="partial" if partial else "success",
                    message=(
                        f"Background media watcher updated with partial source coverage. {new_count} new items."
                        if partial
                        else f"Background media watcher updated successfully. {new_count} new items."
                    ),
                    last_completed_at=utc_now(),
                    last_new_items=new_count,
                    last_error="; ".join(source_failures[:3]),
                    last_partial_count=len(source_failures),
                )
            except Exception as exc:
                LOGGER.warning("Background media watcher failed", extra={"error": str(exc)})
                _set_media_state(
                    status="failed",
                    message=f"Background media watcher failed. {exc}",
                    last_completed_at="",
                    last_error=str(exc),
                )
            time.sleep(interval_seconds)

    MEDIA_THREAD = threading.Thread(target=_runner, name="background-media-watcher", daemon=True)
    MEDIA_THREAD.start()


def parse_args() -> argparse.Namespace:
    """Parse GUI command-line options."""
    parser = argparse.ArgumentParser(description="Local browser GUI for the VEX monitor")
    parser.add_argument("--host", default="", help="Bind host override.")
    parser.add_argument("--port", type=int, default=0, help="Bind port override.")
    parser.add_argument("--log-level", default="", help="Optional log level override.")
    return parser.parse_args()


def main() -> None:
    """GUI entrypoint."""
    args = parse_args()
    settings = load_settings()
    if args.host:
        settings.gui_host = args.host
    if args.port:
        settings.gui_port = args.port
    if args.log_level:
        settings.log_level = args.log_level.upper()
    configure_logging(settings.log_dir, settings.log_level)
    LOGGER.info("Starting GUI", extra={"event": settings.event_sku, "team": settings.team_number})
    app = create_app()
    httpd = make_server(settings.gui_host, settings.gui_port, app)
    _set_refresh_state(status="queued", message="Opening stored data first. Background refresh runs every 10 minutes.")
    _set_media_state(status="queued", message="Background media watcher queued.")
    start_competition_watcher()
    start_media_watcher()
    LOGGER.info(
        "GUI listening",
        extra={"event": f"http://{settings.gui_host}:{settings.gui_port}", "team": settings.team_number},
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("GUI stopped by operator")


if __name__ == "__main__":
    main()
