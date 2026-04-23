"""CLI and scheduler entrypoint for the VEX monitoring agent."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
import httpx

from collectors.media_web import MediaWebCollector
from collectors.robotevents import RobotEventsCollector, RobotEventsResult
from collectors.vexvia_local import VexViaLocalCollector
from config import Settings, load_settings
from notify.discord import (
    send_match_alerts,
    send_media_alerts,
    send_power_rank_alert,
    send_rank_alert,
    send_skills_alert,
)
from reporters.json_export import write_json_export
from reporters.markdown import write_markdown_report
from reporters.static_site import export_static_site, publish_to_git_repo
from storage.db import (
    build_dashboard_view,
    compute_and_store_derived_metrics,
    db_session,
    generate_ai_rankings_snapshot,
    get_latest_team_skill,
    get_previous_snapshot,
    get_previous_team_power,
    init_db,
    insert_media_items,
    record_collector_run,
    record_competition_snapshot,
    record_division_rankings,
    record_skills_snapshot,
    upsert_division_matches,
    upsert_matches,
    utc_now,
)
from utils.logging import configure_logging

LOGGER = logging.getLogger(__name__)


def _power_weights(settings: Settings) -> dict[str, float]:
    """Return the configured power-rank weights."""
    return {
        "official": settings.power_rank_weight_official,
        "opr": settings.power_rank_weight_opr,
        "dpr": settings.power_rank_weight_dpr,
        "ccwm": settings.power_rank_weight_ccwm,
        "skills": settings.power_rank_weight_skills,
        "form": settings.power_rank_weight_form,
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
            ai_rankings = generate_ai_rankings_snapshot(connection, settings.team_number)
            view = build_dashboard_view(connection, settings.team_number)
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
    with db_session(settings.db_path) as connection:
        init_db(connection)
        view = build_dashboard_view(connection, settings.team_number)
        markdown_path = write_markdown_report(settings.reports_dir, view)
        json_path = write_json_export(settings.reports_dir, view)
        return {"markdown": str(markdown_path), "json": str(json_path)}


def build_current_view(settings: Settings) -> dict[str, object]:
    """Load the latest dashboard view from SQLite."""
    with db_session(settings.db_path) as connection:
        init_db(connection)
        return build_dashboard_view(connection, settings.team_number)


def write_static_site(settings: Settings) -> dict[str, str]:
    """Render the static site bundle from the latest stored view."""
    view = build_current_view(settings)
    return export_static_site(settings.base_dir, settings, view)


def run_static_publish(settings: Settings) -> dict[str, object]:
    """Refresh local state, export the static site, and optionally push it."""
    results: dict[str, object] = {"refresh": {}, "reports": {}, "site": {}, "publish": {}}
    for cycle_name, runner in (
        ("competition", run_competition_cycle),
        ("media", run_media_cycle),
    ):
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
    results["publish"] = publish_to_git_repo(settings)
    return results


def run_ai_rankings_cycle(settings: Settings) -> dict[str, object]:
    """Generate and persist the latest hourly AI rankings synthesis."""
    started_at = utc_now()
    with db_session(settings.db_path) as connection:
        init_db(connection)
        try:
            payload = generate_ai_rankings_snapshot(connection, settings.team_number)
            record_collector_run(
                connection,
                "ai_rankings",
                started_at,
                utc_now(),
                True,
                1,
            )
            return payload
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
    scheduler.add_job(run_daily_summary, "cron", hour=settings.daily_summary_hour, minute=0, args=[settings], id="daily_summary")
    return scheduler


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="VEX Worlds monitoring agent")
    parser.add_argument("--once", action="store_true", help="Run one full cycle and exit.")
    parser.add_argument(
        "--collector",
        choices=("all", "robotevents", "media", "ai_rankings"),
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
        run_full_cycle(settings)
        return
    scheduler = build_scheduler(settings)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        LOGGER.info("Scheduler stopped by operator")


if __name__ == "__main__":
    main()
