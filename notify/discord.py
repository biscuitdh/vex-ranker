"""Discord notification helper."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import httpx

from config import Settings
from storage.db import alert_already_sent, record_alert

LOGGER = logging.getLogger(__name__)


CONFIDENCE_ORDER = {"unverified": 0, "trusted": 1, "official": 2}


def confidence_allowed(settings: Settings, item: dict[str, Any]) -> bool:
    """Return whether a media item is above the configured confidence threshold."""
    confidence = str(item.get("confidence", "unverified")).lower()
    return CONFIDENCE_ORDER.get(confidence, 0) >= settings.notify_confidence_rank


def make_alert_key(category: str, value: str) -> str:
    """Build a deterministic alert key."""
    return hashlib.sha256(f"{category}:{value}".encode("utf-8")).hexdigest()


def send_discord_message(
    settings: Settings,
    payload: dict[str, Any],
    client: httpx.Client | None = None,
) -> None:
    """Send a JSON payload to Discord when enabled."""
    if not settings.discord_webhook_url:
        return
    managed_client = client or httpx.Client(timeout=settings.request_timeout_seconds)
    should_close = client is None
    try:
        response = managed_client.post(settings.discord_webhook_url, json=payload)
        response.raise_for_status()
    finally:
        if should_close:
            managed_client.close()


def send_rank_alert(
    connection,
    settings: Settings,
    latest: dict[str, Any] | None,
    delta: dict[str, Any],
    client: httpx.Client | None = None,
) -> bool:
    """Notify Discord about an official rank change."""
    if not settings.discord_webhook_url or not latest:
        return False
    if delta.get("rank_change") in (None, 0):
        return False
    alert_key = make_alert_key("rank", str(latest.get("fetched_at")))
    if alert_already_sent(connection, alert_key):
        return False
    direction = delta.get("rank_direction", "changed")
    payload = {
        "content": (
            f"Team {settings.team_number} official rank update: now #{latest.get('rank')} "
            f"in {settings.division_name} ({direction})."
        )
    }
    send_discord_message(settings, payload, client=client)
    record_alert(connection, alert_key, "rank")
    return True


def send_power_rank_alert(
    connection,
    settings: Settings,
    latest: dict[str, Any] | None,
    delta: dict[str, Any],
    client: httpx.Client | None = None,
) -> bool:
    """Notify Discord about a derived power rank change."""
    if not settings.discord_webhook_url or not latest:
        return False
    if delta.get("power_rank_change") in (None, 0):
        return False
    alert_key = make_alert_key("power_rank", f"{latest.get('snapshot_at')}:{latest.get('team_number')}")
    if alert_already_sent(connection, alert_key):
        return False
    payload = {
        "content": (
            f"Team {settings.team_number} power rank update: now #{latest.get('power_rank')} "
            f"({delta.get('power_rank_direction', 'changed')})."
        )
    }
    send_discord_message(settings, payload, client=client)
    record_alert(connection, alert_key, "power_rank")
    return True


def send_skills_alert(
    connection,
    settings: Settings,
    latest: dict[str, Any] | None,
    previous: dict[str, Any] | None,
    client: httpx.Client | None = None,
) -> bool:
    """Notify Discord about a skills total increase."""
    if not settings.discord_webhook_url or not latest:
        return False
    current_total = float(latest.get("total_score") or 0.0)
    previous_total = float((previous or {}).get("total_score") or 0.0)
    if current_total <= previous_total:
        return False
    alert_key = make_alert_key("skills", f"{latest.get('snapshot_at', '')}:{latest.get('team_number')}")
    if alert_already_sent(connection, alert_key):
        return False
    payload = {
        "content": (
            f"Team {settings.team_number} skills update: total score now {current_total:g} "
            f"(driver {float(latest.get('driver_score') or 0.0):g}, programming {float(latest.get('programming_score') or 0.0):g})."
        )
    }
    send_discord_message(settings, payload, client=client)
    record_alert(connection, alert_key, "skills")
    return True


def send_match_alerts(
    connection,
    settings: Settings,
    matches: list[dict[str, Any]],
    client: httpx.Client | None = None,
) -> int:
    """Notify Discord about completed focal team matches."""
    if not settings.discord_webhook_url:
        return 0
    sent = 0
    for match in matches:
        alert_key = make_alert_key("match", match["match_key"])
        if alert_already_sent(connection, alert_key):
            continue
        payload = {
            "content": (
                f"New completed match for {settings.team_number}: "
                f"{match.get('round_label', match['match_key'])} "
                f"{match.get('score_for', '?')}-{match.get('score_against', '?')} "
                f"vs {match.get('opponent', 'TBD')}."
            )
        }
        send_discord_message(settings, payload, client=client)
        record_alert(connection, alert_key, "match")
        sent += 1
    return sent


def send_media_alerts(
    connection,
    settings: Settings,
    items: list[dict[str, Any]],
    client: httpx.Client | None = None,
) -> int:
    """Notify Discord about newly discovered media items."""
    if not settings.discord_webhook_url:
        return 0
    sent = 0
    for item in items:
        if not confidence_allowed(settings, item):
            continue
        alert_key = make_alert_key("media", item["canonical_key"])
        if alert_already_sent(connection, alert_key):
            continue
        platform = item.get("platform") or item.get("source_type") or "source"
        payload = {
            "content": (
                f"New {platform} mention for {settings.team_number}: "
                f"{item['title']} - {item['url']}"
            )
        }
        send_discord_message(settings, payload, client=client)
        record_alert(connection, alert_key, "media")
        sent += 1
    return sent
