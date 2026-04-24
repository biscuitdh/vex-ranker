"""Discord bridge helpers for interactive away-from-Mac approvals."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from typing import Any

import httpx

from config import Settings

LOGGER = logging.getLogger(__name__)

DISCORD_API_BASE = "https://discord.com/api/v10"
DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
BRIDGE_POLL_INTERVAL_SECONDS = 5
DISCORD_INTENT_GUILDS = 1 << 0
DISCORD_INTENT_GUILD_MESSAGES = 1 << 9
DISCORD_INTENT_MESSAGE_CONTENT = 1 << 15
BUTTON_ACTIONS = {
    "approve": {"label": "Approve", "style": 3},
    "deny": {"label": "Deny", "style": 4},
    "need_info": {"label": "Need Info", "style": 2},
}


def discord_webhook_valid(settings: Settings) -> bool:
    """Return whether the webhook looks like a Discord webhook URL."""
    webhook = settings.discord_webhook_url.strip()
    return webhook.startswith("https://discord.com/api/webhooks/") or webhook.startswith("https://discordapp.com/api/webhooks/")


def discord_bridge_missing_fields(settings: Settings) -> list[str]:
    """Return bridge settings that are still missing."""
    missing: list[str] = []
    if not settings.discord_bot_token:
        missing.append("DISCORD_BOT_TOKEN")
    if not settings.discord_channel_id:
        missing.append("DISCORD_CHANNEL_ID")
    if not settings.discord_allowed_user_ids:
        missing.append("DISCORD_ALLOWED_USER_IDS")
    return missing


def discord_bridge_configured(settings: Settings) -> bool:
    """Return whether the interactive Discord bridge is fully configured."""
    return not discord_bridge_missing_fields(settings)


def discord_configuration_issues(settings: Settings) -> list[str]:
    """Return startup/configuration issues for Discord delivery paths."""
    issues: list[str] = []
    if not settings.discord_webhook_url:
        issues.append("DISCORD_WEBHOOK_URL is not configured; passive Discord alerts are disabled.")
    elif not discord_webhook_valid(settings):
        issues.append("DISCORD_WEBHOOK_URL does not look like a Discord webhook URL.")
    missing_bridge = discord_bridge_missing_fields(settings)
    if missing_bridge:
        issues.append(
            "Interactive Discord bridge is incomplete; missing "
            + ", ".join(missing_bridge)
            + "."
        )
    if discord_bridge_configured(settings) and not settings.discord_application_id:
        issues.append("DISCORD_APPLICATION_ID is not configured; button interactions still work, but diagnostics are thinner.")
    if discord_bridge_configured(settings) and not settings.discord_public_key:
        issues.append("DISCORD_PUBLIC_KEY is not configured; local Gateway mode works, but webhook-style interaction validation is unavailable.")
    return issues


def _bot_headers(settings: Settings) -> dict[str, str]:
    """Return bot-token headers for Discord API calls."""
    if not settings.discord_bot_token:
        raise RuntimeError("DISCORD_BOT_TOKEN is not configured.")
    return {
        "Authorization": f"Bot {settings.discord_bot_token}",
        "Content-Type": "application/json",
    }


def _button_custom_id(request_id: str, action: str) -> str:
    """Build the Discord custom_id for one request action."""
    return f"vexranker:{request_id}:{action}"


def parse_discord_button_custom_id(custom_id: str) -> dict[str, str] | None:
    """Parse one Discord button custom_id into a structured action."""
    raw = custom_id.strip()
    if not raw:
        return None
    parts = raw.split(":")
    if len(parts) != 3 or parts[0] != "vexranker":
        return None
    request_id = parts[1].strip()
    action = parts[2].strip().lower()
    if not request_id or action not in BUTTON_ACTIONS:
        return None
    return {"request_id": request_id, "action": action}


def build_discord_action_components(request_id: str) -> list[dict[str, Any]]:
    """Return button components for one request."""
    return [
        {
            "type": 1,
            "components": [
                {
                    "type": 2,
                    "style": spec["style"],
                    "label": spec["label"],
                    "custom_id": _button_custom_id(request_id, action),
                }
                for action, spec in BUTTON_ACTIONS.items()
            ],
        }
    ]


def _resolved_discord_components(request_id: str, action: str) -> list[dict[str, Any]]:
    """Return disabled buttons after a request has been resolved."""
    components = build_discord_action_components(request_id)
    for row in components:
        for button in row["components"]:
            button["disabled"] = True
            if parse_discord_button_custom_id(str(button["custom_id"]))["action"] == action:
                button["style"] = BUTTON_ACTIONS[action]["style"]
    return components


def render_discord_request_message(request: dict[str, Any], approval_prefix: str) -> str:
    """Render one outbound request message for the private Discord channel."""
    timeout_minutes = int(request.get("timeout_minutes") or 0)
    request_id = str(request.get("request_id") or "unknown")
    prompt = str(request.get("prompt_text") or "").strip()
    allowed_actions = request.get("allowed_actions") or []
    action_summary = ", ".join(str(item) for item in allowed_actions) if allowed_actions else "no remote actions"
    fallback_lines = ""
    if approval_prefix:
        fallback_lines = (
            "\nFallback text replies if you need them:\n"
            f"- `{approval_prefix} {request_id}`\n"
            f"- `deny {request_id}`\n"
            f"- `answer {request_id}: <text>`"
        )
    return (
        f"Action needed for `{request_id}`\n"
        f"{prompt}\n\n"
        f"Allowed action set: {action_summary}\n"
        f"Timeout: {timeout_minutes} minutes.\n"
        "Use the buttons below for normal operation."
        f"{fallback_lines}"
    )


def send_discord_channel_message(
    settings: Settings,
    content: str,
    *,
    components: list[dict[str, Any]] | None = None,
    embeds: list[dict[str, Any]] | None = None,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Post a message to the configured Discord channel via bot token."""
    if not discord_bridge_configured(settings):
        raise RuntimeError(
            "Interactive Discord bridge is not fully configured: "
            + ", ".join(discord_bridge_missing_fields(settings))
        )
    payload: dict[str, Any] = {"content": content}
    if components:
        payload["components"] = components
    if embeds:
        payload["embeds"] = embeds
    managed_client = client or httpx.Client(timeout=settings.request_timeout_seconds)
    should_close = client is None
    try:
        response = managed_client.post(
            f"{DISCORD_API_BASE}/channels/{settings.discord_channel_id}/messages",
            headers=_bot_headers(settings),
            json=payload,
        )
        response.raise_for_status()
        return response.json()
    finally:
        if should_close:
            managed_client.close()


def fetch_channel_messages(
    settings: Settings,
    *,
    limit: int = 25,
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent messages from the configured Discord channel."""
    if not discord_bridge_configured(settings):
        return []
    managed_client = client or httpx.Client(timeout=settings.request_timeout_seconds)
    should_close = client is None
    try:
        response = managed_client.get(
            f"{DISCORD_API_BASE}/channels/{settings.discord_channel_id}/messages",
            headers=_bot_headers(settings),
            params={"limit": max(1, min(limit, 100))},
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []
    finally:
        if should_close:
            managed_client.close()


def parse_discord_reply(text: str, approval_prefix: str = "approve") -> dict[str, str] | None:
    """Parse a narrow Discord reply format into a structured action."""
    raw = text.strip()
    if not raw:
        return None
    lower = raw.lower()
    approve_token = approval_prefix.strip().lower() or "approve"
    if lower.startswith(f"{approve_token} "):
        request_id = raw[len(approve_token) :].strip()
        if request_id:
            return {"action": "approve", "request_id": request_id, "answer_text": ""}
        return None
    if lower.startswith("deny "):
        request_id = raw[5:].strip()
        if request_id:
            return {"action": "deny", "request_id": request_id, "answer_text": ""}
        return None
    if lower.startswith("answer "):
        remainder = raw[7:].strip()
        if ":" not in remainder:
            return None
        request_id, answer_text = remainder.split(":", 1)
        request_id = request_id.strip()
        answer_text = answer_text.strip()
        if request_id and answer_text:
            return {"action": "answer", "request_id": request_id, "answer_text": answer_text}
    return None


def post_discord_request(
    settings: Settings,
    request: dict[str, Any],
    *,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Post a persisted Discord request into the configured private channel."""
    content = render_discord_request_message(request, settings.discord_approval_prefix)
    return send_discord_channel_message(
        settings,
        content,
        components=build_discord_action_components(str(request.get("request_id") or "")),
        client=client,
    )


def post_discord_followup(
    settings: Settings,
    content: str,
    *,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Post a short follow-up message into the Discord control channel."""
    return send_discord_channel_message(settings, content, client=client)


def _interaction_callback_url(interaction_id: str, interaction_token: str) -> str:
    """Return the interaction callback URL for one Discord interaction."""
    return f"{DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"


def _interaction_response_content(request_id: str, action: str, answer_text: str = "") -> str:
    """Return a human-readable button response."""
    action_label = BUTTON_ACTIONS[action]["label"]
    if action == "need_info":
        return (
            f"Request `{request_id}` marked as `{action_label}`. "
            "The monitor will stay in safe mode until a new request is issued."
        )
    suffix = f" Note: {answer_text}" if answer_text else ""
    return f"Request `{request_id}` recorded as `{action_label}`.{suffix}"


def _send_interaction_callback(
    settings: Settings,
    interaction_id: str,
    interaction_token: str,
    payload: dict[str, Any],
    *,
    client: httpx.Client | None = None,
) -> None:
    """Send an interaction callback to Discord."""
    managed_client = client or httpx.Client(timeout=settings.request_timeout_seconds)
    should_close = client is None
    try:
        response = managed_client.post(
            _interaction_callback_url(interaction_id, interaction_token),
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        response.raise_for_status()
    finally:
        if should_close:
            managed_client.close()


def handle_discord_interaction(
    settings: Settings,
    payload: dict[str, Any],
    *,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Persist one button click from Discord and acknowledge it."""
    from storage.db import apply_discord_reply, db_session, get_discord_request_by_request_id

    interaction_id = str(payload.get("id") or "")
    interaction_token = str(payload.get("token") or "")
    interaction_type = int(payload.get("type") or 0)
    actor_id = str(((payload.get("member") or {}).get("user") or {}).get("id") or ((payload.get("user") or {}).get("id") or ""))
    if interaction_type != 3:
        return {"accepted": False, "reason": "unsupported_interaction_type"}
    if actor_id not in settings.discord_allowed_user_ids:
        if interaction_id and interaction_token:
            _send_interaction_callback(
                settings,
                interaction_id,
                interaction_token,
                {"type": 4, "data": {"content": "You are not allowed to approve this request.", "flags": 64}},
                client=client,
            )
        return {"accepted": False, "reason": "unauthorized_user"}

    parsed = parse_discord_button_custom_id(str(((payload.get("data") or {}).get("custom_id")) or ""))
    if not parsed:
        if interaction_id and interaction_token:
            _send_interaction_callback(
                settings,
                interaction_id,
                interaction_token,
                {"type": 4, "data": {"content": "That control is not recognized.", "flags": 64}},
                client=client,
            )
        return {"accepted": False, "reason": "invalid_custom_id"}

    request_id = parsed["request_id"]
    action = parsed["action"]
    message_id = str(((payload.get("message") or {}).get("id")) or interaction_id or "")
    answer_text = ""
    raw_text = BUTTON_ACTIONS[action]["label"]
    if action == "need_info":
        answer_text = "Operator requested more information before approving the action."

    with db_session(settings.db_path) as connection:
        request = get_discord_request_by_request_id(connection, request_id)
        result = apply_discord_reply(
            connection,
            {
                "request_id": request_id,
                "discord_user_id": actor_id,
                "discord_message_id": message_id,
                "raw_text": raw_text,
                "parsed_action": action,
                "answer_text": answer_text,
                "response_source": "button",
                "discord_interaction_id": interaction_id,
                "interaction_custom_id": _button_custom_id(request_id, action),
                "received_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
                "raw_payload": payload,
            },
        )
        updated_request = result.get("request") or request or {}

    if interaction_id and interaction_token:
        if result.get("accepted"):
            response_content = _interaction_response_content(request_id, action, answer_text)
            callback_payload = {
                "type": 7,
                "data": {
                    "content": response_content,
                    "components": _resolved_discord_components(request_id, action),
                },
            }
        else:
            callback_payload = {
                "type": 4,
                "data": {
                    "content": f"Request `{request_id}` could not be updated: {result.get('reason')}.",
                    "flags": 64,
                },
            }
        _send_interaction_callback(settings, interaction_id, interaction_token, callback_payload, client=client)

    if not result.get("accepted"):
        return {"accepted": False, "reason": str(result.get("reason") or "unknown"), "request": updated_request}
    return {"accepted": True, "reason": "recorded", "request": updated_request, "action": action}


def wait_for_discord_resolution(
    settings: Settings,
    request_id: str,
    timeout_minutes: int,
) -> dict[str, Any] | None:
    """Wait for a Discord request to reach a terminal state."""
    from storage.db import db_session, get_discord_request_by_request_id

    deadline = time.monotonic() + (max(1, timeout_minutes) * 60)
    terminal_statuses = {"approved", "denied", "answered", "expired"}
    while time.monotonic() <= deadline:
        with db_session(settings.db_path) as connection:
            request = get_discord_request_by_request_id(connection, request_id)
        if request and str(request.get("status") or "").lower() in terminal_statuses:
            return request
        time.sleep(BRIDGE_POLL_INTERVAL_SECONDS)
    with db_session(settings.db_path) as connection:
        return get_discord_request_by_request_id(connection, request_id)


def run_bridge_once(settings: Settings) -> dict[str, Any]:
    """Expire old requests and optionally process text fallback replies."""
    from storage.db import (
        apply_discord_reply,
        db_session,
        expire_pending_discord_requests,
        init_db,
    )

    processed = 0
    expired = 0
    followups = 0
    with httpx.Client(timeout=settings.request_timeout_seconds) as client:
        if settings.discord_text_fallback_enabled:
            messages = fetch_channel_messages(settings, client=client)
            for message in reversed(messages):
                author = message.get("author") or {}
                author_id = str(author.get("id") or "")
                if not author_id or author.get("bot"):
                    continue
                if author_id not in settings.discord_allowed_user_ids:
                    continue
                parsed = parse_discord_reply(str(message.get("content") or ""), settings.discord_approval_prefix)
                if not parsed:
                    continue
                reply_payload = {
                    "request_id": parsed["request_id"],
                    "discord_user_id": author_id,
                    "discord_message_id": str(message.get("id") or ""),
                    "raw_text": str(message.get("content") or ""),
                    "parsed_action": parsed["action"],
                    "answer_text": parsed.get("answer_text", ""),
                    "response_source": "text",
                    "received_at": str(message.get("timestamp") or ""),
                    "raw_payload": message,
                }
                with db_session(settings.db_path) as connection:
                    result = apply_discord_reply(connection, reply_payload)
                if result.get("accepted"):
                    processed += 1
                    request = result.get("request") or {}
                    post_discord_followup(
                        settings,
                        f"Recorded `{result.get('status')}` for `{request.get('request_id')}` via text fallback.",
                        client=client,
                    )
                    followups += 1
        expired_requests = expire_discord_requests_once(settings)
        for request in expired_requests:
            expired += 1
            post_discord_followup(
                settings,
                f"Request `{request.get('request_id')}` timed out. No risky action was taken.",
                client=client,
            )
            followups += 1
    return {"processed_replies": processed, "expired_requests": expired, "followups": followups}


def expire_discord_requests_once(settings: Settings) -> list[dict[str, Any]]:
    """Expire pending requests without scraping channel history."""
    from storage.db import db_session, expire_pending_discord_requests, init_db

    with db_session(settings.db_path) as connection:
        init_db(connection)
        return expire_pending_discord_requests(connection)


async def _gateway_heartbeat_loop(websocket: Any, interval_ms: int) -> None:
    """Send Gateway heartbeats forever."""
    interval_seconds = max(1.0, interval_ms / 1000.0)
    await asyncio.sleep(random.uniform(0, interval_seconds))
    while True:
        await websocket.send(json.dumps({"op": 1, "d": None}))
        await asyncio.sleep(interval_seconds)


async def _discord_gateway_session(settings: Settings) -> None:
    """Run one Discord Gateway session and persist button interactions."""
    try:
        import websockets
    except ImportError as exc:
        raise RuntimeError("The 'websockets' package is required for Discord button interactions.") from exc

    intents = DISCORD_INTENT_GUILDS | DISCORD_INTENT_GUILD_MESSAGES | DISCORD_INTENT_MESSAGE_CONTENT
    async with websockets.connect(DISCORD_GATEWAY_URL, ping_interval=None) as websocket:
        hello = json.loads(await websocket.recv())
        heartbeat_interval = int((hello.get("d") or {}).get("heartbeat_interval") or 45000)
        heartbeat_task = asyncio.create_task(_gateway_heartbeat_loop(websocket, heartbeat_interval))
        identify_payload = {
            "op": 2,
            "d": {
                "token": settings.discord_bot_token,
                "intents": intents,
                "properties": {"os": "macOS", "browser": "vex-ranker", "device": "vex-ranker"},
            },
        }
        await websocket.send(json.dumps(identify_payload))
        try:
            while True:
                message = json.loads(await websocket.recv())
                op = int(message.get("op") or 0)
                event_name = str(message.get("t") or "")
                data = message.get("d") or {}
                if op == 11:
                    continue
                if op == 7:
                    raise RuntimeError("Discord requested a reconnect.")
                if op == 9:
                    raise RuntimeError("Discord invalidated the Gateway session.")
                if op != 0:
                    continue
                if event_name == "INTERACTION_CREATE":
                    try:
                        handle_discord_interaction(settings, data)
                    except Exception as exc:
                        LOGGER.warning("Discord interaction handling failed", extra={"error": str(exc)})
                if event_name == "MESSAGE_CREATE" and settings.discord_text_fallback_enabled:
                    author = data.get("author") or {}
                    author_id = str(author.get("id") or "")
                    if not author_id or author.get("bot") or author_id not in settings.discord_allowed_user_ids:
                        continue
                    parsed = parse_discord_reply(str(data.get("content") or ""), settings.discord_approval_prefix)
                    if not parsed:
                        continue
                    from storage.db import apply_discord_reply, db_session

                    with db_session(settings.db_path) as connection:
                        apply_discord_reply(
                            connection,
                            {
                                "request_id": parsed["request_id"],
                                "discord_user_id": author_id,
                                "discord_message_id": str(data.get("id") or ""),
                                "raw_text": str(data.get("content") or ""),
                                "parsed_action": parsed["action"],
                                "answer_text": parsed.get("answer_text", ""),
                                "response_source": "text",
                                "received_at": str(data.get("timestamp") or ""),
                                "raw_payload": data,
                            },
                        )
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task


async def _discord_bridge_supervisor(settings: Settings) -> None:
    """Run the Gateway session alongside timeout expiration."""
    async def _expiration_loop() -> None:
        while True:
            try:
                expired_requests = await asyncio.to_thread(expire_discord_requests_once, settings)
                if expired_requests:
                    with httpx.Client(timeout=settings.request_timeout_seconds) as client:
                        for request in expired_requests:
                            post_discord_followup(
                                settings,
                                f"Request `{request.get('request_id')}` timed out. No risky action was taken.",
                                client=client,
                            )
            except Exception as exc:
                LOGGER.warning("Discord request expiration sweep failed", extra={"error": str(exc)})
            await asyncio.sleep(BRIDGE_POLL_INTERVAL_SECONDS)

    expiration_task = asyncio.create_task(_expiration_loop())
    while True:
        try:
            await _discord_gateway_session(settings)
        except Exception as exc:
            LOGGER.warning("Discord Gateway session failed", extra={"error": str(exc)})
            await asyncio.sleep(BRIDGE_POLL_INTERVAL_SECONDS)
        if expiration_task.done():
            break
    with contextlib.suppress(asyncio.CancelledError):
        expiration_task.cancel()
        await expiration_task


def run_bridge_loop(settings: Settings) -> None:
    """Run the Discord bridge loop forever."""
    if not discord_bridge_configured(settings):
        raise RuntimeError(
            "Interactive Discord bridge is not fully configured: "
            + ", ".join(discord_bridge_missing_fields(settings))
        )
    LOGGER.info(
        "Starting Discord bridge",
        extra={
            "channel_id": settings.discord_channel_id,
            "allowed_user_ids": settings.discord_allowed_user_ids,
            "text_fallback_enabled": settings.discord_text_fallback_enabled,
        },
    )
    asyncio.run(_discord_bridge_supervisor(settings))
