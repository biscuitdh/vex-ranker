"""Heuristic analysis summaries for the dashboard and reports."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _source_health(rankings_status: dict[str, Any], collector_runs: list[dict[str, Any]]) -> str:
    """Summarize collector/source health."""
    source = rankings_status.get("snapshot_source") or "unknown"
    source_state = rankings_status.get("source_state") or "unknown"
    source_updated_at = rankings_status.get("source_updated_at") or ""
    result_tabs = rankings_status.get("result_tabs") or {}
    latest_run = collector_runs[0] if collector_runs else None
    if source == "api":
        return "Official API standings are active."
    if source == "vex_via_local":
        freshness = f" Cache last updated {source_updated_at}." if source_updated_at else ""
        return f"VEX Via local cache is active for official standings and match context.{freshness}"
    if source == "results_tab_browser":
        successful_tabs = ", ".join(result_tabs.get("standings", []) + result_tabs.get("skills", []) + result_tabs.get("division_matches", []))
        if successful_tabs:
            return f"Official public results were scraped from results tabs: {successful_tabs}. Current source state is {source_state}."
        return f"Official public results were observed through targeted results-tab scraping. Current source state is {source_state}."
    if source == "division_list_pdf":
        text = "Only the public RECF division roster is available right now, so official live ranks are still unavailable."
        if latest_run and latest_run.get("error_summary"):
            return f"{text} Latest collector warning: {latest_run.get('error_summary')}"
        return text
    return "No reliable official standings source is active yet."


def _automation_health(view: dict[str, Any]) -> str:
    """Summarize self-heal status for operators."""
    dashboard_health = view.get("dashboard_health") or {}
    status = str(dashboard_health.get("status") or "unknown")
    reason = dashboard_health.get("reason_summary") or "No health summary available."
    repair = view.get("last_repair_attempt") or {}
    restart = view.get("last_restart_event") or {}
    parts = [f"Dashboard automation status is {status}."]
    parts.append(str(reason))
    if repair:
        parts.append(
            f"Latest repair attempt finished {repair.get('status', 'unknown')} at {repair.get('completed_at') or 'unknown time'}."
        )
    if restart:
        parts.append(
            f"Latest restart escalation is {restart.get('status', 'unknown')} at {restart.get('requested_at') or 'unknown time'}."
        )
    return " ".join(parts)


def _team_brief(view: dict[str, Any]) -> str:
    """Build a focal-team summary paragraph."""
    snapshot = view.get("latest_snapshot")
    power = view.get("team_power")
    skill = view.get("team_skill")
    if not snapshot:
        return "Team 7157B has no current focal snapshot yet, so the console is operating in discovery mode."
    parts = [
        f"Team {snapshot.get('team_number')} is being tracked in the {snapshot.get('division_name')} Division.",
        f"Current official position is #{snapshot.get('rank') or 'N/A'} with record {snapshot.get('record_text') or 'unknown'}.",
    ]
    if power:
        parts.append(
            f"Derived power rank is #{power.get('power_rank')} with OPR {power.get('opr')}, DPR {power.get('dpr')}, and CCWM {power.get('ccwm')}."
        )
    if skill:
        parts.append(
            f"Latest skills total is {skill.get('total_score')} from driver {skill.get('driver_score')} and programming {skill.get('programming_score')}."
        )
    return " ".join(parts)


def _division_brief(view: dict[str, Any]) -> str:
    """Summarize division coverage and ranking context."""
    rankings = view.get("division_rankings") or []
    power_rankings = view.get("power_rankings") or []
    if not rankings:
        return "No division standings are loaded yet."
    top = rankings[:3]
    leaders = ", ".join(
        f"#{item.get('rank') or '?'} {item.get('team_number')}" for item in top
    )
    text = f"The console currently has {len(rankings)} division rows loaded. Top official teams in view: {leaders}."
    if power_rankings:
        top_power = ", ".join(
            f"#{item.get('power_rank')} {item.get('team_number')}" for item in power_rankings[:3]
        )
        text += f" Top derived power teams: {top_power}."
    result_tabs = (view.get("rankings_status") or {}).get("result_tabs") or {}
    if result_tabs.get("standings") or result_tabs.get("skills") or result_tabs.get("division_matches"):
        text += (
            f" Results-tab coverage: standings from {', '.join(result_tabs.get('standings') or ['none'])}, "
            f"skills from {', '.join(result_tabs.get('skills') or ['none'])}, "
            f"matches from {', '.join(result_tabs.get('division_matches') or ['none'])}."
        )
    return text


def _confidence_brief(view: dict[str, Any]) -> str:
    """Summarize ranking confidence based on source quality and coverage."""
    rankings_status = view.get("rankings_status") or {}
    snapshot_source = rankings_status.get("snapshot_source") or "unknown"
    source_state = rankings_status.get("source_state") or "unknown"
    if snapshot_source == "vex_via_local":
        return "Ranking confidence is high because the dashboard is reading the locally cached VEX Via standings feed."
    if source_state == "live":
        return "Ranking confidence is high because live standings data is available."
    if source_state == "partial":
        return "Ranking confidence is moderate because some competition-relevant data was scraped, but coverage is incomplete."
    if source_state == "roster_only":
        return "Ranking confidence is low for official rank because the system currently has roster-only fallback rather than live standings."
    return "Ranking confidence is unknown because no standings source is active yet."


def _changes_brief(view: dict[str, Any]) -> str:
    """Summarize deltas and recent movement."""
    delta = view.get("delta") or {}
    power_delta = view.get("power_delta") or {}
    recent_completed = view.get("recent_completed_matches") or []
    upcoming = view.get("upcoming_matches") or []
    movers = view.get("biggest_movers") or []
    rank_trend = view.get("rank_trend") or {}
    power_trend = view.get("power_trend") or {}
    parts = [
        f"Official rank direction is {delta.get('rank_direction', 'unknown')}.",
        f"Power rank direction is {power_delta.get('power_rank_direction', 'unknown')}.",
        f"There are {len(recent_completed)} recent completed focal-team matches and {len(upcoming)} upcoming matches stored.",
    ]
    if rank_trend.get("history"):
        parts.append(f"Official-rank trend over {len(rank_trend['history'])} snapshots is {rank_trend.get('direction', 'flat')}.")
    if power_trend.get("history"):
        parts.append(f"Power-rank trend over {len(power_trend['history'])} snapshots is {power_trend.get('direction', 'flat')}.")
    if movers:
        mover = movers[0]
        parts.append(
            f"Biggest recent derived mover is {mover.get('team_number')} with movement {mover.get('movement')}."
        )
    return " ".join(parts)


def _momentum_brief(view: dict[str, Any]) -> str:
    """Summarize current ranking momentum and freshness."""
    rank_trend = view.get("rank_trend") or {}
    power_trend = view.get("power_trend") or {}
    rankings_status = view.get("rankings_status") or {}
    source_updated_at = rankings_status.get("source_updated_at") or "unknown"
    source = rankings_status.get("snapshot_source") or "unknown"
    parts = [f"Current rankings source is {source}, last refreshed at {source_updated_at}."]
    if rank_trend.get("history"):
        latest = rank_trend["history"][-1].get("rank")
        earliest = rank_trend["history"][0].get("rank")
        parts.append(f"Official rank moved from {earliest} to {latest} across the stored trend window.")
    if power_trend.get("history"):
        latest = power_trend["history"][-1].get("power_rank")
        earliest = power_trend["history"][0].get("power_rank")
        parts.append(f"Derived power rank moved from {earliest} to {latest} across the stored trend window.")
    return " ".join(parts)


def _media_brief(view: dict[str, Any]) -> str:
    """Summarize public mention activity."""
    media = view.get("recent_media") or []
    if not media:
        return "No recent public media mentions are stored."
    official = sum(1 for item in media if item.get("confidence") == "official")
    trusted = sum(1 for item in media if item.get("confidence") == "trusted")
    latest = media[0]
    source = latest.get("platform") or latest.get("source") or "unknown source"
    return (
        f"There are {len(media)} recent public mentions in storage, including {official} official and {trusted} trusted items. "
        f"Latest mention came from {source}: {latest.get('title')}."
    )


def _match_brief(view: dict[str, Any]) -> str:
    """Summarize the next and last focal-team match with opponent context."""
    intelligence = view.get("match_intelligence") or {}
    next_match = intelligence.get("next_match") or {}
    last_match = intelligence.get("last_match") or {}
    parts: list[str] = []
    if next_match:
        opponents = ", ".join(next_match.get("opponent_teams") or ["TBD"])
        parts.append(
            f"Next known match is {next_match.get('round_label') or next_match.get('match_key')} against {opponents} at {next_match.get('scheduled_time') or 'TBD'}."
        )
        if next_match.get("opponent_average_official_rank") is not None:
            parts.append(
                f"Those opponents average official rank {next_match.get('opponent_average_official_rank')} and average power rank {next_match.get('opponent_average_power_rank') or 'N/A'}."
            )
    if last_match:
        parts.append(
            f"Last completed match was {last_match.get('round_label') or last_match.get('match_key')} with score {last_match.get('score_for', '?')}-{last_match.get('score_against', '?')}."
        )
    if not parts:
        return "No focal-team match intelligence is available yet."
    return " ".join(parts)


def _threat_brief(view: dict[str, Any]) -> str:
    """Summarize the highest-priority division threats."""
    threats = view.get("threat_list") or []
    if not threats:
        return "No division threat list is available yet."
    top = threats[:3]
    formatted = ", ".join(
        f"{item.get('team_number')} (official #{item.get('official_rank') or 'N/A'}, power #{item.get('power_rank') or 'N/A'})"
        for item in top
    )
    return f"Closest current division threats to 7157B: {formatted}."


def _alliance_brief(view: dict[str, Any]) -> str:
    """Summarize partner and opponent impact."""
    impact = view.get("alliance_impact") or {}
    partners = impact.get("partner_rows") or []
    opponents = impact.get("opponent_rows") or []
    if not partners and not opponents:
        return "No completed-match alliance impact data is available yet."
    parts: list[str] = []
    if partners:
        best_partner = partners[0]
        parts.append(
            f"Best observed partner effect is with {best_partner.get('team_number')} at average margin {best_partner.get('average_margin')} across {best_partner.get('matches')} matches."
        )
    if opponents:
        toughest = opponents[0]
        parts.append(
            f"Toughest observed opponent pressure is from {toughest.get('team_number')} at average margin {toughest.get('average_margin')} across {toughest.get('matches')} matches."
        )
    return " ".join(parts)


def _swing_brief(view: dict[str, Any]) -> str:
    """Summarize the highest-impact upcoming matches."""
    swing_matches = view.get("swing_matches") or []
    if not swing_matches:
        return "No scheduled swing matches are available yet."
    top = swing_matches[0]
    opponents = ", ".join(top.get("opponent_teams") or ["TBD"])
    return (
        f"Highest-impact upcoming swing match is {top.get('round_label') or top.get('match_key')} "
        f"against {opponents} at {top.get('scheduled_time') or 'TBD'} with swing score {top.get('swing_score')}."
    )


def build_analysis(view: dict[str, Any]) -> dict[str, Any]:
    """Build dashboard-friendly analysis cards and a headline summary."""
    rankings_status = view.get("rankings_status") or {}
    collector_runs = view.get("collector_runs") or []
    cards = [
        {
            "title": "Team Outlook",
            "body": _team_brief(view),
        },
        {
            "title": "Division Context",
            "body": _division_brief(view),
        },
        {
            "title": "Change Watch",
            "body": _changes_brief(view),
        },
        {
            "title": "Momentum",
            "body": _momentum_brief(view),
        },
        {
            "title": "Source Health",
            "body": _source_health(rankings_status, collector_runs),
        },
        {
            "title": "Automation Health",
            "body": _automation_health(view),
        },
        {
            "title": "Media Signal",
            "body": _media_brief(view),
        },
        {
            "title": "Match Intelligence",
            "body": _match_brief(view),
        },
        {
            "title": "Threat List",
            "body": _threat_brief(view),
        },
        {
            "title": "Alliance Impact",
            "body": _alliance_brief(view),
        },
        {
            "title": "Swing Matches",
            "body": _swing_brief(view),
        },
        {
            "title": "Ranking Confidence",
            "body": _confidence_brief(view),
        },
    ]
    headline = " ".join(card["body"] for card in cards[:3])
    return {
        "headline": headline,
        "cards": cards,
    }


def build_ai_rankings(view: dict[str, Any]) -> dict[str, Any]:
    """Build a rankings-specific synthesized scouting brief."""
    snapshot = view.get("latest_snapshot") or {}
    power = view.get("team_power") or {}
    skill = view.get("team_skill") or {}
    rankings_status = view.get("rankings_status") or {}
    threat_list = view.get("threat_list") or []
    swing_matches = view.get("swing_matches") or []
    alliance_impact = view.get("alliance_impact") or {}
    movers = view.get("biggest_movers") or []
    rank_trend = view.get("rank_trend") or {}
    power_trend = view.get("power_trend") or {}
    match_intelligence = view.get("match_intelligence") or {}

    generated_at = datetime.now(timezone.utc).isoformat()
    source_snapshot_at = (
        rankings_status.get("source_updated_at")
        or rankings_status.get("latest_rankings_snapshot_at")
        or snapshot.get("fetched_at")
    )
    source_type = rankings_status.get("snapshot_source") or "unknown"
    confidence_body = _confidence_brief(view)
    if "high" in confidence_body.lower():
        confidence_level = "high"
    elif "moderate" in confidence_body.lower():
        confidence_level = "moderate"
    elif "low" in confidence_body.lower():
        confidence_level = "low"
    else:
        confidence_level = "unknown"

    official_rank = snapshot.get("rank")
    power_rank = power.get("power_rank")
    skills_total = skill.get("total_score")
    record_text = snapshot.get("record_text") or "unknown"
    rank_direction = (view.get("delta") or {}).get("rank_direction", "unknown")
    power_direction = (view.get("power_delta") or {}).get("power_rank_direction", "unknown")

    if snapshot:
        headline = (
            f"7157B sits at official rank #{official_rank or 'N/A'} "
            f"and power rank #{power_rank or 'N/A'} with {confidence_level} confidence."
        )
        why_it_matters = (
            f"Current record is {record_text}; official trend is {rank_direction} and derived trend is {power_direction}. "
            f"This matters because nearby Technology teams can compress the standings quickly once more qualification data lands."
        )
    else:
        headline = "7157B does not have enough current standings data for an AI rankings brief yet."
        why_it_matters = "The monitor needs at least one focal-team standings snapshot before it can synthesize a useful rankings brief."

    top_threat = threat_list[0] if threat_list else None
    top_swing = swing_matches[0] if swing_matches else None
    best_partner = (alliance_impact.get("partner_rows") or [None])[0]
    toughest_opponent = (alliance_impact.get("opponent_rows") or [None])[0]
    next_match = match_intelligence.get("next_match") or {}

    summary_blocks = [
        {
            "title": "7157B Outlook",
            "body": _team_brief(view),
        },
        {
            "title": "Why This Rank Matters",
            "body": why_it_matters,
        },
        {
            "title": "Threat Pressure",
            "body": _threat_brief(view),
        },
        {
            "title": "Alliance Effects",
            "body": _alliance_brief(view),
        },
        {
            "title": "Likely Swing Match",
            "body": _swing_brief(view),
        },
        {
            "title": "Source Confidence",
            "body": confidence_body,
        },
    ]

    priority_factors: list[str] = []
    if top_threat:
        priority_factors.append(
            f"Top nearby threat is {top_threat.get('team_number')} at {top_threat.get('threat_level')} level "
            f"with threat score {top_threat.get('threat_score')} "
            f"(official {top_threat.get('official_pressure')}, power {top_threat.get('power_pressure')}, "
            f"skills {top_threat.get('skills_pressure')}, scoring {top_threat.get('scoring_pressure')})."
        )
    if top_swing:
        opponents = ", ".join(top_swing.get("opponent_teams") or ["TBD"])
        priority_factors.append(
            f"Highest swing match is {top_swing.get('round_label') or top_swing.get('match_key')} against {opponents} "
            f"with swing score {top_swing.get('swing_score')}. {top_swing.get('ai_call')}"
        )
    if best_partner:
        priority_factors.append(
            f"Best observed partner fit is {best_partner.get('team_number')} at average margin {best_partner.get('average_margin')}."
        )
    if toughest_opponent:
        priority_factors.append(
            f"Toughest observed opponent pressure is {toughest_opponent.get('team_number')} at average margin {toughest_opponent.get('average_margin')}."
        )
    if next_match:
        opponents = ", ".join(next_match.get("opponent_teams") or ["TBD"])
        priority_factors.append(
            f"Next known match is {next_match.get('round_label') or next_match.get('match_key')} against {opponents} at {next_match.get('scheduled_time') or 'TBD'}."
        )
    if not priority_factors:
        priority_factors.append("Not enough fresh standings and match context are loaded yet to prioritize ranking factors.")

    movers_relevant = [
        item for item in movers
        if item.get("team_number") != snapshot.get("team_number")
    ][:5]

    return {
        "generated_at": generated_at,
        "source_snapshot_at": source_snapshot_at,
        "source_type": source_type,
        "headline": headline,
        "why_it_matters": why_it_matters,
        "official_rank": official_rank,
        "power_rank": power_rank,
        "skills_total": skills_total,
        "record_text": record_text,
        "summary_blocks": summary_blocks,
        "priority_factors": priority_factors,
        "threat_rows": threat_list[:5],
        "swing_rows": swing_matches[:5],
        "alliance": {
            "best_partner": best_partner,
            "toughest_opponent": toughest_opponent,
        },
        "top_movers": movers_relevant,
        "trend": {
            "official_direction": rank_trend.get("direction", "flat"),
            "power_direction": power_trend.get("direction", "flat"),
        },
        "confidence": {
            "level": confidence_level,
            "body": confidence_body,
        },
    }
