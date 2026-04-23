"""Markdown report generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from storage.db import utc_now


def _delta_text(snapshot: dict[str, Any] | None, delta: dict[str, Any], power_row: dict[str, Any] | None, power_delta: dict[str, Any]) -> str:
    """Build the focal-team headline sentence for the report."""
    if not snapshot:
        return "No competition snapshot is available yet."
    if delta.get("rank_change") in (None, 0):
        official_change = "no change from previous check"
    else:
        official_change = f"{delta.get('rank_direction', 'changed')} from previous check"
    if power_row and power_delta.get("power_rank_change") not in (None, 0):
        power_text = (
            f" Power Rank is #{power_row.get('power_rank')} "
            f"({power_delta.get('power_rank_direction', 'changed')})."
        )
    elif power_row:
        power_text = f" Power Rank is #{power_row.get('power_rank')}."
    else:
        power_text = ""
    return (
        f"Team {snapshot.get('team_number')} is currently ranked #{snapshot.get('rank') or 'N/A'} "
        f"in the {snapshot.get('division_name')} Division ({official_change}).{power_text}"
    )


def render_markdown_report(view: dict[str, Any]) -> str:
    """Render a markdown report from the dashboard view."""
    snapshot = view["latest_snapshot"]
    delta = view["delta"]
    power_row = view.get("team_power")
    power_delta = view.get("power_delta", {})
    recent_completed = view["recent_completed_matches"]
    upcoming = view["upcoming_matches"]
    media = view["recent_media"]
    lines = [
        f"# VEX Monitor Report - {utc_now()}",
        "",
        _delta_text(snapshot, delta, power_row, power_delta),
        "",
        f"Automated analysis: {view.get('analysis', {}).get('headline', 'No analysis available.')}",
        "",
    ]
    ai_rankings = view.get("ai_rankings") or {}
    if ai_rankings:
        lines.extend(
            [
                f"AI rankings: {ai_rankings.get('headline', 'No AI rankings available.')}",
                "",
            ]
        )
    lines.extend(
        [
        f"Since the last update: {len(recent_completed)} new completed matches, "
        f"{len(upcoming)} upcoming matches, {len(media)} recent public media mentions.",
        "",
        "## Team 7157B Summary",
        ]
    )
    if snapshot:
        lines.extend(
            [
                f"- Event: {snapshot.get('event_name')}",
                f"- Team: {snapshot.get('team_number')} ({snapshot.get('team_name') or 'Unknown'})",
                f"- Record: {snapshot.get('record_text') or 'Unknown'}",
                f"- WP/AP/SP: {snapshot.get('wp')}/{snapshot.get('ap')}/{snapshot.get('sp')}",
                f"- Average score: {snapshot.get('average_score')}",
                f"- Last fetch: {snapshot.get('fetched_at')}",
            ]
        )
    else:
        lines.append("- No snapshot data yet.")
    if power_row:
        lines.extend(
            [
                f"- Power Rank: #{power_row.get('power_rank')}",
                f"- OPR/DPR/CCWM: {power_row.get('opr')}/{power_row.get('dpr')}/{power_row.get('ccwm')}",
                f"- Recent form: {power_row.get('recent_form')}",
            ]
        )
    if view.get("team_skill"):
        skill = view["team_skill"]
        lines.append(
            f"- Skills: total {skill.get('total_score')} (driver {skill.get('driver_score')}, programming {skill.get('programming_score')})"
        )

    lines.extend(["", "## AI Rankings"])
    if ai_rankings:
        lines.extend(
            [
                f"- Generated: {ai_rankings.get('generated_at')}",
                f"- Source snapshot: {ai_rankings.get('source_snapshot_at') or 'Unknown'}",
                f"- Source type: {ai_rankings.get('source_type') or 'Unknown'}",
                f"- Confidence: {ai_rankings.get('confidence', {}).get('level', 'unknown')}",
                f"- Why it matters: {ai_rankings.get('why_it_matters', 'Unknown')}",
            ]
        )
        for item in ai_rankings.get("priority_factors", [])[:5]:
            lines.append(f"- Priority: {item}")
    else:
        lines.append("- No AI rankings snapshot yet.")

    lines.extend(["", "## Top Official Division Standings"])
    lines.append(
        f"- Rankings ingestion: {view.get('rankings_status', {}).get('rankings_count', 0)} teams, "
        f"{view.get('rankings_status', {}).get('skills_count', 0)} skills rows, "
        f"{view.get('rankings_status', {}).get('power_count', 0)} power rows, "
        f"source {view.get('rankings_status', {}).get('snapshot_source', 'unknown') or 'unknown'}, "
        f"state {view.get('rankings_status', {}).get('source_state', 'unknown') or 'unknown'}"
    )
    if view.get("rankings_status", {}).get("source_updated_at"):
        lines.append(f"- Source last updated: {view['rankings_status']['source_updated_at']}")
    lines.append(
        f"- Result-tab coverage: standings {', '.join(view.get('rankings_status', {}).get('result_tabs', {}).get('standings', [])) or 'none'}, "
        f"skills {', '.join(view.get('rankings_status', {}).get('result_tabs', {}).get('skills', [])) or 'none'}, "
        f"matches {', '.join(view.get('rankings_status', {}).get('result_tabs', {}).get('division_matches', [])) or 'none'}"
    )
    if view.get("division_rankings"):
        for item in view["division_rankings"][:10]:
            lines.append(
                f"- #{item.get('rank')} {item.get('team_number')} - {item.get('record_text')} "
                f"(WP {item.get('wp')}, AP {item.get('ap')}, SP {item.get('sp')})"
            )
    else:
        lines.append("- No division standings yet.")

    lines.extend(["", "## Top Power Rankings"])
    if view.get("power_rankings"):
        for item in view["power_rankings"][:10]:
            lines.append(
                f"- #{item.get('power_rank')} {item.get('team_number')} - score {item.get('composite_score')} "
                f"(OPR {item.get('opr')}, DPR {item.get('dpr')}, CCWM {item.get('ccwm')})"
            )
    else:
        lines.append("- No power rankings yet.")

    lines.extend(["", "## Recent Completed Matches"])
    if recent_completed:
        for match in recent_completed:
            lines.append(
                f"- {match.get('round_label') or match['match_key']}: "
                f"{match.get('score_for', '?')}-{match.get('score_against', '?')} "
                f"vs {match.get('opponent', 'TBD')} ({match.get('completed_time') or 'unknown time'})"
            )
    else:
        lines.append("- None yet.")

    lines.extend(["", "## Upcoming Matches"])
    if upcoming:
        for match in upcoming:
            lines.append(
                f"- {match.get('round_label') or match['match_key']} vs {match.get('opponent', 'TBD')} "
                f"at {match.get('scheduled_time') or 'TBD'}"
            )
    else:
        lines.append("- None scheduled yet.")

    lines.extend(["", "## Upcoming Matchup Slate"])
    if view.get("matchup_summary"):
        lines.append(f"- {view['matchup_summary'].get('headline')}")
    if view.get("upcoming_matchups"):
        for item in view["upcoming_matchups"][:5]:
            lines.append(
                f"- {item.get('round_label') or item.get('match_key')} at {item.get('scheduled_time') or 'TBD'} "
                f"on {item.get('field_name') or 'TBD'} vs {', '.join(item.get('opponent_teams') or []) or item.get('opponent', 'TBD')} "
                f"(opp avg #{item.get('opponent_average_official_rank') or 'N/A'} / P#{item.get('opponent_average_power_rank') or 'N/A'}) "
                f"- {item.get('matchup_call')}"
            )
    else:
        lines.append("- No known upcoming 7157B matchup slate is available from the local cache.")

    lines.extend(["", "## Recent Media"])
    if media:
        for item in media:
            lines.append(
                f"- [{item['title']}]({item['url']}) - {item['platform'] or item['source']} - {item['confidence']}"
            )
    else:
        lines.append("- No media items yet.")
    return "\n".join(lines) + "\n"


def write_markdown_report(reports_dir: Path, view: dict[str, Any]) -> Path:
    """Write a timestamped markdown report and update latest.md."""
    rendered = render_markdown_report(view)
    timestamp = utc_now().replace(":", "-")
    target = reports_dir / f"summary-{timestamp}.md"
    latest = reports_dir / "latest.md"
    target.write_text(rendered, encoding="utf-8")
    latest.write_text(rendered, encoding="utf-8")
    return target
