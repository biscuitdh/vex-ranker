"""CLI and scheduler entrypoint for the VEX monitoring agent."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
import httpx

from collectors.media_web import MediaWebCollector
from collectors.robotevents import RobotEventsCollector, RobotEventsResult
from collectors.vexvia_local import VexViaLocalCollector
from config import Settings, load_settings
from notify.discord import (
    send_health_transition_alert,
    send_match_alerts,
    send_media_alerts,
    send_power_rank_alert,
    send_rank_alert,
    send_skills_alert,
)
from notify.discord_bridge import (
    discord_bridge_configured,
    discord_configuration_issues,
    post_discord_request,
    wait_for_discord_resolution,
)
from reporters.json_export import write_json_export
from reporters.markdown import write_markdown_report
from reporters.static_site import export_static_site, publish_to_git_repo
from storage.db import (
    build_dashboard_view,
    compute_and_store_derived_metrics,
    db_session,
    evaluate_dashboard_health,
    create_discord_request,
    get_available_teams,
    get_latest_healthcheck_run,
    get_discord_request_by_request_id,
    generate_ai_rankings_snapshot,
    get_latest_restart_event,
    get_latest_team_skill,
    parse_timestamp,
    get_previous_snapshot,
    get_previous_team_power,
    init_db,
    insert_media_items,
    record_collector_run,
    record_competition_snapshot,
    record_division_rankings,
    record_healthcheck_run,
    record_repair_attempt,
    record_restart_event,
    record_skills_snapshot,
    mark_discord_request_posted,
    update_discord_request_status,
    upsert_division_matches,
    upsert_matches,
    utc_now,
)
from utils.logging import configure_logging
from utils.runtime_lock import runtime_lock
from utils.service_control import restart_managed_services

LOGGER = logging.getLogger(__name__)


def _is_locked_error(exc: Exception) -> bool:
    """Return whether an exception represents SQLite lock contention."""
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()


def _run_with_lock_retry(
    action_name: str,
    runner,
    settings: Settings,
    *,
    attempts: int = 3,
    delay_seconds: float = 1.0,
) -> object:
    """Run one action with a short retry loop for SQLite lock contention."""
    for attempt in range(1, attempts + 1):
        try:
            return runner(settings)
        except Exception as exc:
            if not _is_locked_error(exc) or attempt == attempts:
                raise
            LOGGER.warning(
                "Action hit a transient SQLite lock; retrying",
                extra={"action": action_name, "attempt": attempt, "error": str(exc)},
            )
            time.sleep(delay_seconds * attempt)


def _generate_ai_rankings_for_event_teams(connection, settings: Settings) -> dict[str, dict[str, object]]:
    """Generate AI rankings snapshots for all currently available division teams."""
    payloads: dict[str, dict[str, object]] = {}
    for item in get_available_teams(connection, settings.team_number, limit=250):
        team_number = str(item.get("team_number") or "").strip()
        if not team_number:
            continue
        payloads[team_number] = generate_ai_rankings_snapshot(connection, team_number)
    return payloads


def _health_payload_from_row(row: dict[str, object] | None) -> dict[str, object] | None:
    """Decode a persisted healthcheck payload."""
    if not row or row.get("raw_json") in (None, ""):
        return None
    try:
        import json

        return json.loads(str(row["raw_json"]))
    except Exception:
        return None


def _power_weights(settings: Settings) -> dict[str, float]:
    """Return the configured power-rank weights."""
    return {
        "official": settings.power_rank_weight_official,
        "opr": settings.power_rank_weight_opr,
        "dpr": settings.power_rank_weight_dpr,
        "ccwm": settings.power_rank_weight_ccwm,
        "skills": settings.power_rank_weight_skills,
        "form": settings.power_rank_weight_form,
        "manual": settings.power_rank_weight_manual,
    }


def _merge_tab_metadata(primary: dict[str, object], secondary: dict[str, object]) -> dict[str, object]:
    """Merge result-tab metadata from two sources."""
    merged = {
        "attempted_tabs": sorted(set((primary.get("attempted_tabs") or []) + (secondary.get("attempted_tabs") or []))),
        "successful_tabs": sorted(set((primary.get("successful_tabs") or []) + (secondary.get("successful_tabs") or []))),
        "dataset_tabs": {},
        "request_urls": sorted(set((primary.get("request_urls") or []) + (secondary.get("request_urls") or [])))[:50],
    }
    for key in ("standings", "skills", "matches", "division_matches"):
        primary_tabs = (primary.get("dataset_tabs") or {}).get(key) or []
        secondary_tabs = (secondary.get("dataset_tabs") or {}).get(key) or []
        merged["dataset_tabs"][key] = sorted(set(primary_tabs + secondary_tabs))
    return merged


def _select_competition_result(primary, secondary):
    """Choose the best competition source and merge partial data where sensible."""
    if secondary is None or not secondary.division_rankings:
        return primary
    if primary.snapshot_source == "api":
        primary.skills = primary.skills or secondary.skills
        primary.division_matches = primary.division_matches or secondary.division_matches
        primary.matches = primary.matches or secondary.matches
        primary.snapshot = primary.snapshot or secondary.snapshot
        primary.warnings.extend(secondary.warnings)
        primary.result_tabs = _merge_tab_metadata(primary.result_tabs, secondary.result_tabs)
        return primary
    secondary.warnings = primary.warnings + secondary.warnings
    secondary.result_tabs = _merge_tab_metadata(primary.result_tabs, secondary.result_tabs)
    return secondary


def run_competition_cycle(settings: Settings) -> dict[str, object]:
    """Run the RobotEvents collection cycle."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=180):
        return _run_competition_cycle_unlocked(settings)


def _run_competition_cycle_unlocked(settings: Settings) -> dict[str, object]:
    """Run the RobotEvents collection cycle without acquiring the external runtime lock."""
    started_at = utc_now()
    with db_session(settings.db_path) as connection:
        init_db(connection)
        previous_skill = get_latest_team_skill(connection, settings.team_number)
        collector = RobotEventsCollector(settings)
        local_collector = VexViaLocalCollector(settings) if settings.enable_vexvia_local else None
        try:
            robotevents_error = ""
            try:
                result = collector.fetch()
            except Exception as exc:
                robotevents_error = str(exc)
                LOGGER.warning("RobotEvents collection failed before local fallback merge", extra={"collector": "robotevents", "error": robotevents_error})
                result = RobotEventsResult(
                    snapshot=None,
                    matches=[],
                    division_rankings=[],
                    skills=[],
                    division_matches=[],
                    snapshot_source="unavailable",
                    warnings=[f"RobotEvents unavailable: {robotevents_error}"],
                    result_tabs={
                        "attempted_tabs": [],
                        "successful_tabs": [],
                        "dataset_tabs": {"standings": [], "skills": [], "matches": [], "division_matches": []},
                        "request_urls": [],
                    },
                )
            if local_collector:
                local_started_at = utc_now()
                try:
                    local_result = local_collector.fetch()
                    record_collector_run(
                        connection,
                        "vexvia_local",
                        local_started_at,
                        utc_now(),
                        bool(local_result.division_rankings),
                        len(local_result.division_rankings),
                        "; ".join(local_result.warnings),
                    )
                    result = _select_competition_result(result, local_result)
                except Exception as exc:
                    record_collector_run(
                        connection,
                        "vexvia_local",
                        local_started_at,
                        utc_now(),
                        False,
                        0,
                        str(exc),
                    )
                    LOGGER.warning("VEX Via local cache collection failed", extra={"collector": "vexvia_local", "error": str(exc)})
                    result.warnings.append(f"VEX Via local cache unavailable: {exc}")
            if not result.division_rankings:
                raise RuntimeError("; ".join(result.warnings) or robotevents_error or "No competition data was collected")
            if result.snapshot:
                snapshot_at = result.snapshot["fetched_at"]
                record_competition_snapshot(connection, result.snapshot)
            elif result.division_rankings:
                snapshot_at = str(result.division_rankings[0].get("fetched_at") or utc_now())
            elif result.skills:
                snapshot_at = str(result.skills[0].get("fetched_at") or utc_now())
            else:
                snapshot_at = utc_now()
            match_delta = upsert_matches(connection, result.matches)
            if result.division_rankings:
                record_division_rankings(connection, snapshot_at, result.division_rankings)
            if result.skills:
                record_skills_snapshot(connection, snapshot_at, result.skills)
            if result.division_matches:
                upsert_division_matches(connection, result.division_matches)
            metrics: list[dict[str, object]] = []
            if result.division_rankings:
                metrics = compute_and_store_derived_metrics(
                    connection,
                    snapshot_at=snapshot_at,
                    event_sku=settings.event_sku,
                    division_name=settings.division_name,
                    recent_match_count=settings.power_rank_recent_match_count,
                    weights=_power_weights(settings),
                )
            record_collector_run(
                connection,
                "robotevents",
                started_at,
                utc_now(),
                bool(result.division_rankings),
                len(result.division_rankings),
                "; ".join(result.warnings),
            )
            ai_rankings_by_team = _generate_ai_rankings_for_event_teams(connection, settings)
            ai_rankings = ai_rankings_by_team.get(settings.team_number) or {}
            view = build_dashboard_view(connection, settings.team_number, settings, include_operations=False)
            with httpx.Client(timeout=settings.request_timeout_seconds) as discord_client:
                send_rank_alert(connection, settings, view["latest_snapshot"], view["delta"], client=discord_client)
                send_power_rank_alert(connection, settings, view.get("team_power"), view.get("power_delta", {}), client=discord_client)
                send_skills_alert(connection, settings, view.get("team_skill"), previous_skill, client=discord_client)
                send_match_alerts(connection, settings, match_delta.new_completed, client=discord_client)
            return {
                "snapshot": result.snapshot,
                "new_completed_matches": match_delta.new_completed,
                "new_scheduled_matches": match_delta.new_scheduled,
                "division_rankings": result.division_rankings,
                "skills": result.skills,
                "metrics": metrics,
                "snapshot_source": result.snapshot_source,
                "warnings": result.warnings,
                "result_tabs": result.result_tabs,
                "ai_rankings": ai_rankings,
                "ai_rankings_by_team": ai_rankings_by_team,
            }
        except Exception as exc:
            record_collector_run(
                connection,
                "robotevents",
                started_at,
                utc_now(),
                False,
                0,
                str(exc),
            )
            LOGGER.exception("Competition cycle failed")
            raise
        finally:
            collector.close()


def run_media_cycle(settings: Settings) -> dict[str, object]:
    """Run the public media collection cycle."""
    started_at = utc_now()
    with db_session(settings.db_path) as connection:
        init_db(connection)
        collector = MediaWebCollector(settings)
        try:
            items = collector.fetch()
            inserted = insert_media_items(connection, items)
            failure_count = len(collector.last_failures)
            summary = "; ".join(collector.last_failures[:5]) if collector.last_failures else ""
            record_collector_run(
                connection,
                "media_web",
                started_at,
                utc_now(),
                True,
                len(inserted),
                summary,
            )
            with httpx.Client(timeout=settings.request_timeout_seconds) as discord_client:
                send_media_alerts(connection, settings, inserted, client=discord_client)
            return {"new_media_items": inserted, "source_failures": collector.last_failures, "partial": failure_count > 0}
        except Exception as exc:
            record_collector_run(
                connection,
                "media_web",
                started_at,
                utc_now(),
                False,
                0,
                str(exc),
            )
            LOGGER.exception("Media cycle failed")
            raise
        finally:
            collector.close()


def write_reports(settings: Settings) -> dict[str, str]:
    """Write the latest markdown and JSON reports."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=180):
        with db_session(settings.db_path) as connection:
            init_db(connection)
            view = build_dashboard_view(connection, settings.team_number, settings, include_operations=False)
            markdown_path = write_markdown_report(settings.reports_dir, view)
            json_path = write_json_export(settings.reports_dir, view)
            return {"markdown": str(markdown_path), "json": str(json_path)}


def build_current_view(settings: Settings) -> dict[str, object]:
    """Load the latest dashboard view from SQLite."""
    with db_session(settings.db_path) as connection:
        init_db(connection)
        return build_dashboard_view(connection, settings.team_number, settings)


def build_all_current_views(settings: Settings) -> dict[str, dict[str, object]]:
    """Load the latest dashboard views for all current division teams."""
    with db_session(settings.db_path) as connection:
        init_db(connection)
        views: dict[str, dict[str, object]] = {}
        for item in get_available_teams(connection, settings.team_number, limit=250):
            team_number = str(item.get("team_number") or "").strip()
            if not team_number:
                continue
            views[team_number] = build_dashboard_view(connection, team_number, settings, include_operations=False)
        if settings.team_number not in views:
            views[settings.team_number] = build_dashboard_view(connection, settings.team_number, settings, include_operations=False)
        return views


def write_static_site(settings: Settings) -> dict[str, str]:
    """Render the static site bundle from the latest stored view."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=180):
        started_at = utc_now()
        try:
            team_views = build_all_current_views(settings)
            result = export_static_site(settings.base_dir, settings, team_views[settings.team_number], team_views=team_views)
            with db_session(settings.db_path) as connection:
                init_db(connection)
                record_collector_run(connection, "static_site", started_at, utc_now(), True, len(team_views), "")
            return result
        except Exception as exc:
            with db_session(settings.db_path) as connection:
                init_db(connection)
                record_collector_run(connection, "static_site", started_at, utc_now(), False, 0, str(exc))
            raise


def publish_static_site(settings: Settings) -> dict[str, object]:
    """Publish the current static site when publishing is configured."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=180):
        started_at = utc_now()
        result = publish_to_git_repo(settings)
        success = bool(result.get("published")) or str(result.get("reason") or "") == "No site changes to publish."
        summary = str(result.get("reason") or "")
        with db_session(settings.db_path) as connection:
            init_db(connection)
            record_collector_run(connection, "publish_static", started_at, utc_now(), success, int(bool(result.get("published"))), summary)
        return result


def run_static_publish(settings: Settings) -> dict[str, object]:
    """Refresh local state, export the static site, and optionally push it."""
    results: dict[str, object] = {"refresh": {}, "reports": {}, "site": {}, "publish": {}}
    for cycle_name, runner in (("competition", run_competition_cycle),):
        try:
            results["refresh"][cycle_name] = runner(settings)
        except Exception as exc:
            LOGGER.warning("Static publish refresh step failed", extra={"step": cycle_name, "error": str(exc)})
            results["refresh"][cycle_name] = {"error": str(exc)}
    try:
        results["refresh"]["ai_rankings"] = run_ai_rankings_cycle(settings)
    except Exception as exc:
        LOGGER.warning("Static publish AI rankings refresh failed", extra={"step": "ai_rankings", "error": str(exc)})
        results["refresh"]["ai_rankings"] = {"error": str(exc)}
    results["reports"] = write_reports(settings)
    results["site"] = write_static_site(settings)
    results["publish"] = publish_static_site(settings)
    return results


def run_ai_rankings_cycle(settings: Settings) -> dict[str, object]:
    """Generate and persist the latest hourly AI rankings synthesis."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=180):
        started_at = utc_now()
        with db_session(settings.db_path) as connection:
            init_db(connection)
            try:
                payloads = _generate_ai_rankings_for_event_teams(connection, settings)
                record_collector_run(
                    connection,
                    "ai_rankings",
                    started_at,
                    utc_now(),
                    True,
                    len(payloads),
                )
                return payloads.get(settings.team_number) or next(iter(payloads.values()), {})
            except Exception as exc:
                record_collector_run(
                    connection,
                    "ai_rankings",
                    started_at,
                    utc_now(),
                    False,
                    0,
                    str(exc),
                )
                LOGGER.exception("AI rankings cycle failed")
                raise


def run_dashboard_healthcheck(settings: Settings) -> dict[str, object]:
    """Evaluate dashboard health and persist the result."""
    started_at = utc_now()
    with db_session(settings.db_path) as connection:
        init_db(connection)
        previous_health = _health_payload_from_row(get_latest_healthcheck_run(connection))
        payload = evaluate_dashboard_health(connection, settings)
        completed_at = utc_now()
        healthcheck_id = record_healthcheck_run(
            connection,
            started_at=started_at,
            completed_at=completed_at,
            status=str(payload.get("status") or "unknown"),
            reason_summary=str(payload.get("reason_summary") or ""),
            payload=payload,
        )
        payload["healthcheck_run_id"] = healthcheck_id
        payload["checked_at"] = completed_at
        try:
            with httpx.Client(timeout=settings.request_timeout_seconds) as discord_client:
                send_health_transition_alert(connection, settings, previous_health, payload, client=discord_client)
        except Exception as exc:
            LOGGER.warning("Health transition alert failed", extra={"error": str(exc)})
        return payload


def _restart_allowed(settings: Settings, latest_restart_event: dict[str, object] | None) -> tuple[bool, str]:
    """Return whether a managed restart is currently allowed."""
    if not settings.enable_service_restart:
        return False, "Managed service restart is disabled."
    if not latest_restart_event:
        return True, ""
    requested_at = parse_timestamp(str(latest_restart_event.get("requested_at") or ""))
    if requested_at is None:
        return True, ""
    now = datetime.now(timezone.utc)
    elapsed_minutes = (now - requested_at).total_seconds() / 60.0
    if elapsed_minutes >= settings.restart_cooldown_minutes:
        return True, ""
    return (
        False,
        f"Restart cooldown active for another {round(settings.restart_cooldown_minutes - elapsed_minutes, 2)} minutes.",
    )


def _record_final_health_state(
    connection,
    *,
    settings: Settings,
    started_at: str,
    previous_health: dict[str, object] | None,
    payload: dict[str, object],
) -> dict[str, object]:
    """Persist the final health payload and send any transition alert."""
    completed_at = utc_now()
    healthcheck_id = record_healthcheck_run(
        connection,
        started_at=started_at,
        completed_at=completed_at,
        status=str(payload.get("status") or "unknown"),
        reason_summary=str(payload.get("reason_summary") or ""),
        payload=payload,
    )
    payload["checked_at"] = completed_at
    payload["healthcheck_run_id"] = healthcheck_id
    try:
        with httpx.Client(timeout=settings.request_timeout_seconds) as discord_client:
            send_health_transition_alert(connection, settings, previous_health, payload, client=discord_client)
    except Exception as exc:
        LOGGER.warning("Health transition alert failed", extra={"error": str(exc)})
    return payload


def _local_self_heal_components_healthy(payload: dict[str, object]) -> bool:
    """Return whether the local match-day critical surfaces are healthy enough."""
    components = payload.get("components")
    if not isinstance(components, dict) or not components:
        return bool(payload.get("healthy"))
    critical_components = ("data_pipeline", "match_progress", "gui_surface", "service_supervision")
    for name in critical_components:
        component = components.get(name)
        if not isinstance(component, dict):
            return bool(payload.get("healthy"))
        if str(component.get("status") or "").lower() != "healthy":
            return False
    return True


def _local_self_heal_message(payload: dict[str, object], attempt_number: int) -> str:
    """Build a match-day focused recovery message."""
    if payload.get("healthy"):
        return f"Dashboard recovered after repair attempt {attempt_number}."
    components = payload.get("components")
    if not isinstance(components, dict):
        return f"Local dashboard recovered after repair attempt {attempt_number}."
    degraded_noncritical = []
    for name in ("published_surface", "notification_path"):
        component = components.get(name)
        if isinstance(component, dict) and str(component.get("status") or "").lower() == "degraded":
            degraded_noncritical.append(str(component.get("summary") or name))
    if degraded_noncritical:
        return (
            f"Local dashboard recovered after repair attempt {attempt_number}, "
            f"but non-critical surfaces remain degraded: {'; '.join(degraded_noncritical[:2])}"
        )
    return f"Local dashboard recovered after repair attempt {attempt_number}."


def _log_discord_configuration_status(settings: Settings) -> None:
    """Log actionable Discord configuration issues at startup."""
    for issue in discord_configuration_issues(settings):
        LOGGER.warning(issue)


def _request_discord_restart_approval(
    settings: Settings,
    *,
    healthcheck_run_id: int,
    latest_health: dict[str, object],
) -> dict[str, object]:
    """Create, post, and wait on a Discord restart approval request."""
    reason_summary = str(latest_health.get("reason_summary") or "Dashboard remained unhealthy after repair attempts.")
    prompt = (
        f"Vex Ranker self-heal is still blocked for team {settings.team_number}. "
        f"Reason: {reason_summary} "
        f"Approve a managed restart of backend and GUI services only if you want the monitor to attempt it remotely."
    )
    with db_session(settings.db_path) as connection:
        init_db(connection)
        request = create_discord_request(
            connection,
            category="restart_approval",
            prompt=prompt,
            allowed_actions=["restart_services"],
            timeout_minutes=settings.discord_reply_timeout_minutes,
        )
    try:
        posted = post_discord_request(settings, request)
        with db_session(settings.db_path) as connection:
            init_db(connection)
            request = mark_discord_request_posted(connection, str(request.get("request_id") or ""), str(posted.get("id") or ""))
    except Exception as exc:
        LOGGER.warning("Discord approval request failed to post", extra={"error": str(exc)})
        with db_session(settings.db_path) as connection:
            init_db(connection)
            request = update_discord_request_status(
                connection,
                str(request.get("request_id") or ""),
                "expired",
                response_text=f"Discord request delivery failed: {exc}",
                resolved_at=utc_now(),
                extra_payload={"delivery_error": str(exc), "healthcheck_run_id": healthcheck_run_id},
            )
        return {
            "status": "delivery_failed",
            "message": f"Discord approval request could not be delivered: {exc}",
            "request": request,
        }

    resolved_request = wait_for_discord_resolution(
        settings,
        str(request.get("request_id") or ""),
        settings.discord_reply_timeout_minutes,
    )
    if resolved_request is None:
        with db_session(settings.db_path) as connection:
            init_db(connection)
            resolved_request = get_discord_request_by_request_id(connection, str(request.get("request_id") or ""))
    status = str((resolved_request or {}).get("status") or "expired")
    if status == "pending":
        with db_session(settings.db_path) as connection:
            init_db(connection)
            resolved_request = update_discord_request_status(
                connection,
                str(request.get("request_id") or ""),
                "expired",
                response_text="Timed out waiting for a Discord approval reply.",
                resolved_at=utc_now(),
                extra_payload={"healthcheck_run_id": healthcheck_run_id},
            )
        status = "expired"
    return {
        "status": status,
        "message": str((resolved_request or {}).get("response_text") or ""),
        "request": resolved_request,
    }


def run_self_heal_cycle(settings: Settings) -> dict[str, object]:
    """Check dashboard health, attempt repairs, and escalate to managed restarts if required."""
    with runtime_lock(settings.data_dir, "db-writer", timeout_seconds=240):
        return _run_self_heal_cycle_unlocked(settings)


def _run_self_heal_cycle_unlocked(settings: Settings) -> dict[str, object]:
    """Check dashboard health, attempt repairs, and escalate to managed restarts without the external runtime lock."""
    if not settings.enable_auto_heal:
        return {
            "status": "disabled",
            "message": "Auto-heal is disabled.",
            "repair_attempts": [],
            "restart": {},
        }

    started_at = utc_now()
    LOGGER.info("Self-heal cycle starting", extra={"team": settings.team_number, "event": settings.event_sku})
    with db_session(settings.db_path) as connection:
        LOGGER.info("Self-heal initializing database", extra={"db_path": str(settings.db_path)})
        init_db(connection)
        LOGGER.info("Self-heal loading previous health", extra={"db_path": str(settings.db_path)})
        previous_health = _health_payload_from_row(get_latest_healthcheck_run(connection))
        LOGGER.info("Self-heal evaluating initial health", extra={"team": settings.team_number})
        initial_health = evaluate_dashboard_health(connection, settings)
        LOGGER.info(
            "Self-heal recording initial health",
            extra={"status": str(initial_health.get("status") or "unknown"), "reason": str(initial_health.get("reason_summary") or "")},
        )
        healthcheck_id = record_healthcheck_run(
            connection,
            started_at=started_at,
            completed_at=utc_now(),
            status=str(initial_health.get("status") or "unknown"),
            reason_summary=str(initial_health.get("reason_summary") or ""),
            payload=initial_health,
        )
        LOGGER.info("Self-heal initial health persisted", extra={"healthcheck_run_id": healthcheck_id})

    result: dict[str, object] = {
        "status": str(initial_health.get("status") or "unknown"),
        "initial_health": initial_health,
        "final_health": initial_health,
        "healthcheck_run_id": healthcheck_id,
        "repair_attempts": [],
        "restart": {},
    }
    if initial_health.get("healthy"):
        with db_session(settings.db_path) as connection:
            init_db(connection)
            final_health = _record_final_health_state(
                connection,
                settings=settings,
                started_at=started_at,
                previous_health=previous_health,
                payload=initial_health,
            )
        result["final_health"] = final_health
        result["message"] = "Dashboard health is already within threshold."
        return result

    for attempt_number in range(1, settings.max_auto_repair_attempts + 1):
        attempt_started_at = utc_now()
        actions: list[str] = []
        errors: list[str] = []
        LOGGER.info("Self-heal repair attempt starting", extra={"attempt": attempt_number, "healthcheck_run_id": healthcheck_id})
        for action_name, runner in (
            ("competition", run_competition_cycle),
            ("ai_rankings", run_ai_rankings_cycle),
            ("reports", write_reports),
        ):
            try:
                LOGGER.info("Self-heal running action", extra={"attempt": attempt_number, "action": action_name})
                _run_with_lock_retry(action_name, runner, settings)
                actions.append(action_name)
                LOGGER.info("Self-heal action complete", extra={"attempt": attempt_number, "action": action_name})
            except Exception as exc:
                errors.append(f"{action_name}: {exc}")
                LOGGER.warning(
                    "Self-heal action failed",
                    extra={"action": action_name, "attempt": attempt_number, "error": str(exc)},
                )

        with db_session(settings.db_path) as connection:
            init_db(connection)
            LOGGER.info("Self-heal evaluating intermediate health", extra={"attempt": attempt_number})
            intermediate_health = evaluate_dashboard_health(connection, settings)
        if intermediate_health.get("components", {}).get("gui_surface", {}).get("status") == "failed":
            try:
                LOGGER.info("Self-heal restarting GUI service", extra={"attempt": attempt_number})
                restart_managed_services(settings, ["gui"])
                actions.append("gui_restart")
            except Exception as exc:
                errors.append(f"gui_restart: {exc}")
                LOGGER.warning("Self-heal GUI restart failed", extra={"attempt": attempt_number, "error": str(exc)})

        try:
            LOGGER.info("Self-heal running action", extra={"attempt": attempt_number, "action": "static_site"})
            _run_with_lock_retry("static_site", write_static_site, settings)
            actions.append("static_site")
            LOGGER.info("Self-heal action complete", extra={"attempt": attempt_number, "action": "static_site"})
        except Exception as exc:
            errors.append(f"static_site: {exc}")
            LOGGER.warning("Self-heal action failed", extra={"action": "static_site", "attempt": attempt_number, "error": str(exc)})

        with db_session(settings.db_path) as connection:
            init_db(connection)
            LOGGER.info("Self-heal evaluating post-repair health", extra={"attempt": attempt_number})
            post_health = evaluate_dashboard_health(connection, settings)
            attempt_payload = {
                "attempt_number": attempt_number,
                "actions": actions,
                "errors": errors,
                "post_health": post_health,
            }
            attempt_status = "success" if _local_self_heal_components_healthy(post_health) else "failed"
            LOGGER.info(
                "Self-heal recording repair attempt",
                extra={"attempt": attempt_number, "attempt_status": attempt_status, "actions": actions, "error_count": len(errors)},
            )
            repair_attempt_id = record_repair_attempt(
                connection,
                healthcheck_run_id=healthcheck_id,
                attempt_number=attempt_number,
                started_at=attempt_started_at,
                completed_at=utc_now(),
                status=attempt_status,
                error_summary="; ".join(errors[:6]),
                payload=attempt_payload,
            )
        result["repair_attempts"].append(
            {
                "repair_attempt_id": repair_attempt_id,
                "attempt_number": attempt_number,
                "status": attempt_status,
                "actions": actions,
                "errors": errors,
                "post_health": post_health,
            }
        )
        result["final_health"] = post_health
        result["status"] = str(post_health.get("status") or "unknown")
        if _local_self_heal_components_healthy(post_health):
            with db_session(settings.db_path) as connection:
                init_db(connection)
                LOGGER.info("Self-heal recording recovered final health", extra={"attempt": attempt_number})
                final_health = _record_final_health_state(
                    connection,
                    settings=settings,
                    started_at=attempt_started_at,
                    previous_health=previous_health,
                    payload=post_health,
                )
            result["final_health"] = final_health
            result["message"] = _local_self_heal_message(final_health, attempt_number)
            return result

    with db_session(settings.db_path) as connection:
        init_db(connection)
        LOGGER.info("Self-heal evaluating final health before restart decision", extra={"healthcheck_run_id": healthcheck_id})
        latest_restart_event = get_latest_restart_event(connection)
        latest_health = evaluate_dashboard_health(connection, settings)
        restart_allowed, restart_reason = _restart_allowed(settings, latest_restart_event)

    if _local_self_heal_components_healthy(latest_health):
        with db_session(settings.db_path) as connection:
            init_db(connection)
            LOGGER.info("Self-heal recording locally recovered final health", extra={"healthcheck_run_id": healthcheck_id})
            final_health = _record_final_health_state(
                connection,
                settings=settings,
                started_at=started_at,
                previous_health=previous_health,
                payload=latest_health,
            )
        result["final_health"] = final_health
        result["status"] = str(final_health.get("status") or "unknown")
        result["message"] = _local_self_heal_message(final_health, settings.max_auto_repair_attempts)
        return result

    if not restart_allowed:
        restart_payload = {
            "status": "skipped",
            "message": restart_reason,
            "targets": ["backend", "gui"],
            "results": [],
        }
        with db_session(settings.db_path) as connection:
            init_db(connection)
            restart_event_id = record_restart_event(
                connection,
                healthcheck_run_id=healthcheck_id,
                requested_at=utc_now(),
                completed_at=utc_now(),
                status="skipped",
                reason_summary=restart_reason,
                targets=["backend", "gui"],
                payload=restart_payload,
            )
            final_health = _record_final_health_state(
                connection,
                settings=settings,
                started_at=started_at,
                previous_health=previous_health,
                payload=latest_health,
            )
        restart_payload["restart_event_id"] = restart_event_id
        result["restart"] = restart_payload
        result["final_health"] = final_health
        result["message"] = restart_reason
        return result

    if discord_bridge_configured(settings):
        approval = _request_discord_restart_approval(
            settings,
            healthcheck_run_id=healthcheck_id,
            latest_health=latest_health,
        )
        result["discord_request"] = approval.get("request") or {}
        approval_status = str(approval.get("status") or "expired")
        if approval_status != "approved":
            resolved_action = str(((approval.get("request") or {}).get("last_operator_action")) or "")
            blocker_message = {
                "denied": "Discord denied the managed restart request.",
                "answered": (
                    "Discord requested more information before approving the restart."
                    if resolved_action == "need_info"
                    else "Discord replied with information, but the restart was not approved."
                ),
                "expired": "Discord approval timed out before a managed restart was approved.",
                "delivery_failed": str(approval.get("message") or "Discord approval request could not be delivered."),
            }.get(approval_status, "Managed restart was not approved through Discord.")
            restart_payload = {
                "status": "skipped",
                "message": blocker_message,
                "targets": ["backend", "gui"],
                "results": [],
                "discord_request_id": str((approval.get("request") or {}).get("request_id") or ""),
            }
            with db_session(settings.db_path) as connection:
                init_db(connection)
                restart_event_id = record_restart_event(
                    connection,
                    healthcheck_run_id=healthcheck_id,
                    requested_at=utc_now(),
                    completed_at=utc_now(),
                    status="skipped",
                    reason_summary=blocker_message,
                    targets=["backend", "gui"],
                    payload=restart_payload,
                )
                final_health = _record_final_health_state(
                    connection,
                    settings=settings,
                    started_at=started_at,
                    previous_health=previous_health,
                    payload=latest_health,
                )
            restart_payload["restart_event_id"] = restart_event_id
            result["restart"] = restart_payload
            result["final_health"] = final_health
            result["message"] = blocker_message
            return result

    restart_payload = restart_managed_services(settings, ["backend", "gui"])
    with db_session(settings.db_path) as connection:
        init_db(connection)
        restart_event_id = record_restart_event(
            connection,
            healthcheck_run_id=healthcheck_id,
            requested_at=utc_now(),
            completed_at=utc_now(),
            status=str(restart_payload.get("status") or "unknown"),
            reason_summary=str(latest_health.get("reason_summary") or "Dashboard remained unhealthy after repair attempts."),
            targets=["backend", "gui"],
            payload=restart_payload,
        )
        final_health = _record_final_health_state(
            connection,
            settings=settings,
            started_at=started_at,
            previous_health=previous_health,
            payload=latest_health,
        )
    restart_payload["restart_event_id"] = restart_event_id
    result["restart"] = restart_payload
    result["final_health"] = final_health
    result["status"] = "restart_requested" if restart_payload.get("status") != "failed" else "failed"
    result["message"] = str(restart_payload.get("message") or "Managed service restart requested.")
    return result


def run_full_cycle(settings: Settings) -> dict[str, object]:
    """Run both collectors and generate reports."""
    results: dict[str, object] = {}
    results["competition"] = run_competition_cycle(settings)
    results["media"] = run_media_cycle(settings)
    results["reports"] = write_reports(settings)
    return results


def run_daily_summary(settings: Settings) -> None:
    """Generate daily reports from the latest stored data."""
    write_reports(settings)


def build_scheduler(settings: Settings) -> BlockingScheduler:
    """Build the APScheduler scheduler."""
    scheduler = BlockingScheduler(timezone=ZoneInfo(settings.timezone))
    scheduler.add_job(run_competition_cycle, "interval", minutes=settings.poll_interval_minutes, args=[settings], id="competition")
    scheduler.add_job(run_media_cycle, "interval", minutes=settings.media_interval_minutes, args=[settings], id="media")
    scheduler.add_job(run_ai_rankings_cycle, "interval", hours=1, args=[settings], id="ai_rankings")
    if settings.enable_auto_heal:
        scheduler.add_job(
            run_self_heal_cycle,
            "interval",
            minutes=settings.healthcheck_interval_minutes,
            args=[settings],
            id="self_heal",
        )
    scheduler.add_job(run_daily_summary, "cron", hour=settings.daily_summary_hour, minute=0, args=[settings], id="daily_summary")
    return scheduler


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="VEX Worlds monitoring agent")
    parser.add_argument("--once", action="store_true", help="Run one full cycle and exit.")
    parser.add_argument(
        "--collector",
        choices=("all", "robotevents", "media", "ai_rankings", "self_heal", "healthcheck"),
        default="all",
        help="Run only one collector when using --once.",
    )
    parser.add_argument("--publish-static", action="store_true", help="Refresh local data, export the static site, and optionally push it.")
    parser.add_argument("--log-level", default="", help="Optional log level override.")
    return parser.parse_args()


def main() -> None:
    """Program entrypoint."""
    args = parse_args()
    settings = load_settings()
    if args.log_level:
        settings.log_level = args.log_level.upper()
    configure_logging(settings.log_dir, settings.log_level)
    _log_discord_configuration_status(settings)
    LOGGER.info(
        "Starting monitor",
        extra={"event": settings.event_sku, "team": settings.team_number},
    )
    if args.publish_static:
        result = run_static_publish(settings)
        LOGGER.info("Static publish complete", extra={"site": result.get("site"), "publish": result.get("publish")})
        return
    if args.once:
        if args.collector == "robotevents":
            run_competition_cycle(settings)
            write_reports(settings)
            return
        if args.collector == "media":
            run_media_cycle(settings)
            write_reports(settings)
            return
        if args.collector == "ai_rankings":
            run_ai_rankings_cycle(settings)
            write_reports(settings)
            return
        if args.collector == "self_heal":
            run_self_heal_cycle(settings)
            write_reports(settings)
            return
        if args.collector == "healthcheck":
            run_dashboard_healthcheck(settings)
            return
        run_full_cycle(settings)
        return
    scheduler = build_scheduler(settings)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        LOGGER.info("Scheduler stopped by operator")


if __name__ == "__main__":
    main()
