"""Static site export and optional GitHub Pages publish helpers."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import shutil
import subprocess
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import Settings
from reporters.json_export import render_json_export
from storage.db import utc_now

LOGGER = logging.getLogger(__name__)


PAGE_SPECS: dict[str, tuple[str, str]] = {
    "dashboard": ("index.html", "gui_dashboard.html.j2"),
    "ai_rankings": ("ai-rankings/index.html", "gui_ai_rankings.html.j2"),
    "matches": ("matches/index.html", "gui_matches.html.j2"),
}


def _template_environment(base_dir: Path) -> Environment:
    """Create a Jinja environment for the static exporter."""
    template_dir = base_dir / "templates"
    return Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(enabled_extensions=("html",)),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _relative_href(page_key: str, current_key: str) -> str:
    """Return a relative href from one exported page to another."""
    if current_key == "dashboard":
        prefix = ""
    else:
        prefix = "../"
    target = PAGE_SPECS[page_key][0]
    if page_key == "dashboard":
        return f"{prefix}index.html"
    return f"{prefix}{target[:-10]}/"


def _nav_items(current_key: str) -> list[tuple[str, str, str]]:
    """Build navigation links for the static site."""
    labels = {
        "dashboard": "Dashboard",
        "ai_rankings": "AI Rankings",
        "matches": "Matches",
    }
    return [(key, _relative_href(key, current_key), labels[key]) for key in PAGE_SPECS]


def _status_banner(view: dict[str, Any]) -> dict[str, str]:
    """Build the static header summary."""
    snapshot = view.get("latest_snapshot") or {}
    power_row = view.get("team_power") or {}
    ai_rankings = view.get("ai_rankings") or {}
    if not snapshot:
        return {
            "headline": "No snapshot published yet",
            "subtext": "The site is showing the latest exported state available on the Mac.",
        }
    power_text = f" · Power #{power_row.get('power_rank')}" if power_row else ""
    ai_text = f" · AI {ai_rankings.get('confidence', {}).get('level', '').title()}" if ai_rankings else ""
    return {
        "headline": f"Team {snapshot.get('team_number')} - Rank #{snapshot.get('rank') or 'N/A'}{power_text}{ai_text}",
        "subtext": f"Last source snapshot: {snapshot.get('fetched_at', 'unknown')}",
    }


def _json_safe(value: Any) -> Any:
    """Recursively convert values to JSON-safe primitives."""
    if is_dataclass(value):
        return {key: _json_safe(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _page_payloads(view: dict[str, Any], settings: Settings, exported_at: str) -> dict[str, dict[str, Any]]:
    """Build per-page JSON payloads."""
    base = render_json_export(view)
    payloads: dict[str, dict[str, Any]] = {
        "dashboard": base,
        "ai-rankings": {
            "generated_at": exported_at,
            "ai_rankings": view.get("ai_rankings"),
            "latest_snapshot": view.get("latest_snapshot"),
            "previous_snapshot": view.get("previous_snapshot"),
            "delta": view.get("delta"),
            "match_intelligence": view.get("match_intelligence"),
        },
        "matches": {
            "generated_at": exported_at,
            "match_intelligence": view.get("match_intelligence"),
            "upcoming_matchups": view.get("upcoming_matchups"),
            "matchup_summary": view.get("matchup_summary"),
            "swing_matches": view.get("swing_matches"),
            "alliance_impact": view.get("alliance_impact"),
            "recent_completed_matches": view.get("recent_completed_matches"),
            "upcoming_matches": view.get("upcoming_matches"),
        },
    }
    return {name: _json_safe(payload) for name, payload in payloads.items()}


def _sanitized_static_view(view: dict[str, Any]) -> dict[str, Any]:
    """Strip local-only or noisy values before public export."""
    sanitized = dict(view)
    sanitized["collector_runs"] = [
        {
            **item,
            "error_summary": "Warning present" if item.get("error_summary") else "",
        }
        for item in (view.get("collector_runs") or [])
    ]
    return sanitized


def export_static_site(base_dir: Path, settings: Settings, view: dict[str, Any]) -> dict[str, str]:
    """Render the static dashboard and JSON snapshot bundle."""
    environment = _template_environment(base_dir)
    exported_at = utc_now()
    site_dir = settings.static_site_dir
    if site_dir.exists():
        for child in site_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    site_dir.mkdir(parents=True, exist_ok=True)
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")

    export_view = _sanitized_static_view(view)
    export_metadata = {
        "generated_at": exported_at,
        "base_url": settings.static_site_base_url,
        "source_snapshot_at": (export_view.get("latest_snapshot") or {}).get("fetched_at") or (export_view.get("rankings_status") or {}).get("source_updated_at"),
        "source_type": (export_view.get("rankings_status") or {}).get("snapshot_source") or (export_view.get("latest_snapshot") or {}).get("source") or "unknown",
        "source_state": (export_view.get("rankings_status") or {}).get("source_state") or "unknown",
    }

    written_pages: dict[str, str] = {}
    for page_key, (relative_path, template_name) in PAGE_SPECS.items():
        target = site_dir / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        context = dict(export_view)
        context.update(
            {
                "settings": settings,
                "active_tab": page_key,
                "nav_items": _nav_items(page_key),
                "status_banner": _status_banner(export_view),
                "action_message": "",
                "refresh_state": {},
                "media_state": {},
                "static_site": True,
                "export_metadata": export_metadata,
                "threat_sort": "threat_score",
                "threat_dir": "desc",
                "next_threat_dir": lambda _requested_sort, default_dir="desc": default_dir,
            }
        )
        rendered = environment.get_template(template_name).render(**context)
        target.write_text(rendered, encoding="utf-8")
        written_pages[page_key] = str(target)

    page_payloads = _page_payloads(export_view, settings, exported_at)
    for name, payload in page_payloads.items():
        (data_dir / f"{name}.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (data_dir / "latest.json").write_text(
        json.dumps(_json_safe(render_json_export(export_view)), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"site_dir": str(site_dir), "generated_at": exported_at, "index": str(site_dir / "index.html"), **written_pages}


def _run_git(repo_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command in a target repo."""
    return subprocess.run(
        ["git", "-C", str(repo_path), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _sync_publish_tree(source_dir: Path, target_repo: Path) -> None:
    """Replace the publish repo working tree contents with the generated site."""
    for child in target_repo.iterdir():
        if child.name == ".git":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    for child in source_dir.iterdir():
        destination = target_repo / child.name
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)


def publish_to_git_repo(settings: Settings) -> dict[str, Any]:
    """Sync the generated site into a checked-out Pages repo and optionally push."""
    site_dir = settings.static_site_dir
    repo_path = settings.github_pages_repo
    if repo_path is None:
        return {"published": False, "reason": "GITHUB_PAGES_REPO is not configured."}
    if not site_dir.exists():
        return {"published": False, "reason": f"Static site directory does not exist: {site_dir}"}
    if not (repo_path / ".git").exists():
        return {"published": False, "reason": f"Configured Pages repo is not a git checkout: {repo_path}"}

    try:
        current_branch = _run_git(repo_path, "branch", "--show-current").stdout.strip()
        if settings.publish_branch and not current_branch:
            _run_git(repo_path, "checkout", "-B", settings.publish_branch)
        elif settings.publish_branch and current_branch != settings.publish_branch:
            _run_git(repo_path, "checkout", settings.publish_branch)
        _sync_publish_tree(site_dir, repo_path)
        status = _run_git(repo_path, "status", "--porcelain").stdout.strip()
        if not status:
            return {"published": False, "reason": "No site changes to publish.", "repo": str(repo_path)}
        _run_git(repo_path, "add", "-A")
        commit_message = f"Update VEX static site {datetime.now(timezone.utc).isoformat()}"
        _run_git(repo_path, "commit", "-m", commit_message)
        if settings.git_push_enabled:
            _run_git(repo_path, "push", "origin", settings.publish_branch)
        return {
            "published": True,
            "repo": str(repo_path),
            "branch": settings.publish_branch,
            "pushed": settings.git_push_enabled,
        }
    except subprocess.CalledProcessError as exc:
        LOGGER.warning("Static site publish failed", extra={"repo": str(repo_path), "error": exc.stderr.strip() or exc.stdout.strip()})
        return {
            "published": False,
            "repo": str(repo_path),
            "reason": exc.stderr.strip() or exc.stdout.strip() or str(exc),
        }
