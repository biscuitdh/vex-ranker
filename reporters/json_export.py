"""JSON export generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from storage.db import utc_now


def render_json_export(view: dict[str, Any]) -> dict[str, Any]:
    """Build a machine-readable export payload."""
    return {
        "generated_at": utc_now(),
        "focal_team_snapshot": view["latest_snapshot"],
        "previous_snapshot": view["previous_snapshot"],
        "official_delta": view["delta"],
        "team_power": view.get("team_power"),
        "power_delta": view.get("power_delta"),
        "team_skill": view.get("team_skill"),
        "recent_completed_matches": view["recent_completed_matches"],
        "upcoming_matches": view["upcoming_matches"],
        "recent_media": view["recent_media"],
        "collector_runs": view["collector_runs"][:10],
        "snapshot_history": view["snapshot_history"][:10],
        "analysis": view.get("analysis"),
        "ai_rankings": view.get("ai_rankings"),
        "rankings_status": view.get("rankings_status"),
        "match_intelligence": view.get("match_intelligence"),
        "upcoming_matchups": view.get("upcoming_matchups", [])[:5],
        "matchup_summary": view.get("matchup_summary"),
        "alliance_impact": view.get("alliance_impact"),
        "swing_matches": view.get("swing_matches", [])[:10],
        "threat_list": view.get("threat_list", [])[:10],
        "division_rankings": view.get("division_rankings", [])[:25],
        "skills_rankings": view.get("skills_rankings", [])[:25],
        "power_rankings": view.get("power_rankings", [])[:25],
        "biggest_movers": view.get("biggest_movers", [])[:10],
    }


def write_json_export(reports_dir: Path, view: dict[str, Any]) -> Path:
    """Write a timestamped JSON export and update latest.json."""
    payload = render_json_export(view)
    timestamp = utc_now().replace(":", "-")
    target = reports_dir / f"summary-{timestamp}.json"
    latest = reports_dir / "latest.json"
    rendered = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    target.write_text(rendered, encoding="utf-8")
    latest.write_text(rendered, encoding="utf-8")
    return target
