"""SQLite storage helpers for the monitoring agent."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Iterator

from utils.analysis import build_ai_rankings, build_analysis


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def to_json(value: Any) -> str:
    """Encode a Python value as JSON."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


@dataclass(slots=True)
class MatchDelta:
    """Summary of focal team match changes from one write pass."""

    new_completed: list[dict[str, Any]]
    new_scheduled: list[dict[str, Any]]


def connect_db(db_path: Path) -> sqlite3.Connection:
    """Open the SQLite database with row access."""
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


@contextmanager
def db_session(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Yield a database connection and commit automatically."""
    connection = connect_db(db_path)
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    """Return the set of column names for a table."""
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _add_column_if_missing(
    connection: sqlite3.Connection, table_name: str, column_name: str, definition: str
) -> None:
    """Add a column if it does not already exist."""
    if column_name in _column_names(connection, table_name):
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def init_db(connection: sqlite3.Connection) -> None:
    """Create the required schema if it does not already exist."""
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS competition_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_sku TEXT NOT NULL,
            event_name TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            team_name TEXT,
            school_name TEXT,
            rank INTEGER,
            wins INTEGER,
            losses INTEGER,
            ties INTEGER,
            wp REAL,
            ap REAL,
            sp REAL,
            average_score REAL,
            record_text TEXT,
            source TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_key TEXT NOT NULL UNIQUE,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            match_type TEXT,
            round_label TEXT,
            instance INTEGER,
            status TEXT NOT NULL,
            scheduled_time TEXT,
            completed_time TEXT,
            field_id INTEGER,
            field_name TEXT,
            alliance TEXT,
            opponent TEXT,
            score_for INTEGER,
            score_against INTEGER,
            raw_json TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS media_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_key TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            source TEXT NOT NULL,
            snippet TEXT,
            published_at TEXT,
            discovered_at TEXT NOT NULL,
            confidence TEXT NOT NULL,
            query_term TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS alerts_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_key TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            sent_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS collector_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collector_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            success INTEGER NOT NULL,
            item_count INTEGER NOT NULL,
            error_summary TEXT
        );

        CREATE TABLE IF NOT EXISTS division_rankings_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            team_name TEXT,
            organization TEXT,
            rank INTEGER,
            wins INTEGER,
            losses INTEGER,
            ties INTEGER,
            wp REAL,
            ap REAL,
            sp REAL,
            average_score REAL,
            record_text TEXT,
            raw_json TEXT NOT NULL,
            UNIQUE(snapshot_at, event_sku, division_name, team_number)
        );

        CREATE TABLE IF NOT EXISTS skills_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            team_name TEXT,
            driver_score REAL,
            programming_score REAL,
            total_score REAL,
            source TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE(snapshot_at, event_sku, division_name, team_number)
        );

        CREATE TABLE IF NOT EXISTS division_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_key TEXT NOT NULL UNIQUE,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            match_type TEXT,
            round_label TEXT,
            instance INTEGER,
            status TEXT NOT NULL,
            scheduled_time TEXT,
            completed_time TEXT,
            field_id INTEGER,
            field_name TEXT,
            red_score REAL,
            blue_score REAL,
            red_teams_json TEXT NOT NULL,
            blue_teams_json TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS match_participation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_key TEXT NOT NULL,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            alliance TEXT NOT NULL,
            partner_teams_json TEXT NOT NULL,
            opponent_teams_json TEXT NOT NULL,
            score_for REAL,
            score_against REAL,
            margin REAL,
            status TEXT NOT NULL,
            completed_time TEXT,
            raw_json TEXT NOT NULL,
            UNIQUE(match_key, team_number)
        );

        CREATE TABLE IF NOT EXISTS derived_metrics_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            event_sku TEXT NOT NULL,
            division_name TEXT NOT NULL,
            team_number TEXT NOT NULL,
            official_rank INTEGER,
            skills_total REAL,
            opr REAL,
            dpr REAL,
            ccwm REAL,
            recent_form REAL,
            composite_score REAL,
            power_rank INTEGER,
            raw_json TEXT NOT NULL,
            UNIQUE(snapshot_at, event_sku, division_name, team_number)
        );

        CREATE TABLE IF NOT EXISTS ai_rankings_snapshots (
            team_number TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            source_snapshot_at TEXT,
            source_type TEXT NOT NULL,
            confidence TEXT NOT NULL,
            headline TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        """
    )

    _add_column_if_missing(connection, "media_items", "source_type", "TEXT DEFAULT 'web'")
    _add_column_if_missing(connection, "media_items", "platform", "TEXT DEFAULT ''")
    _add_column_if_missing(connection, "media_items", "author_handle", "TEXT DEFAULT ''")
    _add_column_if_missing(connection, "media_items", "matched_terms", "TEXT DEFAULT '[]'")
    _add_column_if_missing(connection, "media_items", "collector_name", "TEXT DEFAULT ''")
    _add_column_if_missing(connection, "matches", "field_id", "INTEGER")
    _add_column_if_missing(connection, "matches", "field_name", "TEXT")
    _add_column_if_missing(connection, "division_matches", "field_id", "INTEGER")
    _add_column_if_missing(connection, "division_matches", "field_name", "TEXT")


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    """Convert a row to a plain dictionary."""
    return dict(row) if row is not None else None


def record_competition_snapshot(connection: sqlite3.Connection, snapshot: dict[str, Any]) -> int:
    """Persist a focal team competition snapshot and return its row id."""
    cursor = connection.execute(
        """
        INSERT INTO competition_snapshots (
            event_sku, event_name, division_name, team_number, team_name, school_name,
            rank, wins, losses, ties, wp, ap, sp, average_score, record_text, source,
            fetched_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["event_sku"],
            snapshot["event_name"],
            snapshot["division_name"],
            snapshot["team_number"],
            snapshot.get("team_name"),
            snapshot.get("school_name"),
            snapshot.get("rank"),
            snapshot.get("wins"),
            snapshot.get("losses"),
            snapshot.get("ties"),
            snapshot.get("wp"),
            snapshot.get("ap"),
            snapshot.get("sp"),
            snapshot.get("average_score"),
            snapshot.get("record_text"),
            snapshot.get("source", "api"),
            snapshot["fetched_at"],
            to_json(snapshot),
        ),
    )
    return int(cursor.lastrowid)


def record_division_rankings(
    connection: sqlite3.Connection, snapshot_at: str, rankings: list[dict[str, Any]]
) -> None:
    """Persist a division-wide official rankings snapshot."""
    for item in rankings:
        connection.execute(
            """
            INSERT OR REPLACE INTO division_rankings_snapshots (
                snapshot_at, event_sku, division_name, team_number, team_name, organization,
                rank, wins, losses, ties, wp, ap, sp, average_score, record_text, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_at,
                item["event_sku"],
                item["division_name"],
                item["team_number"],
                item.get("team_name"),
                item.get("organization"),
                item.get("rank"),
                item.get("wins"),
                item.get("losses"),
                item.get("ties"),
                item.get("wp"),
                item.get("ap"),
                item.get("sp"),
                item.get("average_score"),
                item.get("record_text"),
                to_json(item),
            ),
        )


def record_skills_snapshot(
    connection: sqlite3.Connection, snapshot_at: str, skills_rows: list[dict[str, Any]]
) -> None:
    """Persist a division skills snapshot."""
    for item in skills_rows:
        connection.execute(
            """
            INSERT OR REPLACE INTO skills_snapshots (
                snapshot_at, event_sku, division_name, team_number, team_name,
                driver_score, programming_score, total_score, source, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_at,
                item["event_sku"],
                item["division_name"],
                item["team_number"],
                item.get("team_name"),
                item.get("driver_score"),
                item.get("programming_score"),
                item.get("total_score"),
                item.get("source", "api"),
                to_json(item),
            ),
        )


def upsert_matches(connection: sqlite3.Connection, matches: list[dict[str, Any]]) -> MatchDelta:
    """Insert or update focal team match records and report newly discovered deltas."""
    new_completed: list[dict[str, Any]] = []
    new_scheduled: list[dict[str, Any]] = []
    now = utc_now()
    for match in matches:
        existing = connection.execute(
            "SELECT status FROM matches WHERE match_key = ?",
            (match["match_key"],),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO matches (
                    match_key, event_sku, division_name, team_number, match_type, round_label,
                    instance, status, scheduled_time, completed_time, field_id, field_name, alliance, opponent,
                    score_for, score_against, raw_json, discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    match["match_key"],
                    match["event_sku"],
                    match["division_name"],
                    match["team_number"],
                    match.get("match_type"),
                    match.get("round_label"),
                    match.get("instance"),
                    match["status"],
                    match.get("scheduled_time"),
                    match.get("completed_time"),
                    match.get("field_id"),
                    match.get("field_name"),
                    match.get("alliance"),
                    match.get("opponent"),
                    match.get("score_for"),
                    match.get("score_against"),
                    to_json(match),
                    now,
                    now,
                ),
            )
            if match["status"] == "completed":
                new_completed.append(match)
            elif match["status"] == "scheduled":
                new_scheduled.append(match)
            continue

        previous_status = existing["status"]
        connection.execute(
            """
            UPDATE matches
            SET status = ?, scheduled_time = ?, completed_time = ?, field_id = ?, field_name = ?, alliance = ?, opponent = ?,
                score_for = ?, score_against = ?, raw_json = ?, updated_at = ?
            WHERE match_key = ?
            """,
            (
                match["status"],
                match.get("scheduled_time"),
                match.get("completed_time"),
                match.get("field_id"),
                match.get("field_name"),
                match.get("alliance"),
                match.get("opponent"),
                match.get("score_for"),
                match.get("score_against"),
                to_json(match),
                now,
                match["match_key"],
            ),
        )
        if previous_status != "completed" and match["status"] == "completed":
            new_completed.append(match)
        if previous_status not in {"completed", "scheduled"} and match["status"] == "scheduled":
            new_scheduled.append(match)
    return MatchDelta(new_completed=new_completed, new_scheduled=new_scheduled)


def upsert_division_matches(connection: sqlite3.Connection, matches: list[dict[str, Any]]) -> None:
    """Insert or update division-wide match records and participation rows."""
    now = utc_now()
    for match in matches:
        connection.execute(
            """
            INSERT OR REPLACE INTO division_matches (
                match_key, event_sku, division_name, match_type, round_label, instance,
                status, scheduled_time, completed_time, field_id, field_name, red_score, blue_score,
                red_teams_json, blue_teams_json, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match["match_key"],
                match["event_sku"],
                match["division_name"],
                match.get("match_type"),
                match.get("round_label"),
                match.get("instance"),
                match["status"],
                match.get("scheduled_time"),
                match.get("completed_time"),
                match.get("field_id"),
                match.get("field_name"),
                match.get("red_score"),
                match.get("blue_score"),
                to_json(match.get("red_teams", [])),
                to_json(match.get("blue_teams", [])),
                to_json(match),
                now,
            ),
        )

        connection.execute("DELETE FROM match_participation WHERE match_key = ?", (match["match_key"],))
        for alliance_name, teams, score_for, opponents, score_against in (
            ("red", match.get("red_teams", []), match.get("red_score"), match.get("blue_teams", []), match.get("blue_score")),
            ("blue", match.get("blue_teams", []), match.get("blue_score"), match.get("red_teams", []), match.get("red_score")),
        ):
            for team_number in teams:
                partner_teams = [team for team in teams if team != team_number]
                connection.execute(
                    """
                    INSERT OR REPLACE INTO match_participation (
                        match_key, event_sku, division_name, team_number, alliance,
                        partner_teams_json, opponent_teams_json, score_for, score_against,
                        margin, status, completed_time, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        match["match_key"],
                        match["event_sku"],
                        match["division_name"],
                        team_number,
                        alliance_name,
                        to_json(partner_teams),
                        to_json(opponents),
                        score_for,
                        score_against,
                        None
                        if score_for is None or score_against is None
                        else float(score_for) - float(score_against),
                        match["status"],
                        match.get("completed_time"),
                        to_json(
                            {
                                "round_label": match.get("round_label"),
                                "score_for": score_for,
                                "score_against": score_against,
                                "partner_teams": partner_teams,
                                "opponent_teams": opponents,
                            }
                        ),
                    ),
                )


def insert_media_items(connection: sqlite3.Connection, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Insert media items and return only newly discovered entries."""
    inserted: list[dict[str, Any]] = []
    for item in items:
        try:
            connection.execute(
                """
                INSERT INTO media_items (
                    canonical_key, title, url, source, snippet, published_at,
                    discovered_at, confidence, query_term, raw_json, source_type,
                    platform, author_handle, matched_terms, collector_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["canonical_key"],
                    item["title"],
                    item["url"],
                    item["source"],
                    item.get("snippet"),
                    item.get("published_at"),
                    item["discovered_at"],
                    item["confidence"],
                    item["query_term"],
                    to_json(item),
                    item.get("source_type", "web"),
                    item.get("platform", ""),
                    item.get("author_handle", ""),
                    to_json(item.get("matched_terms", [])),
                    item.get("collector_name", ""),
                ),
            )
        except sqlite3.IntegrityError:
            continue
        inserted.append(item)
    return inserted


def alert_already_sent(connection: sqlite3.Connection, alert_key: str) -> bool:
    """Return whether an alert fingerprint has already been recorded."""
    row = connection.execute("SELECT 1 FROM alerts_sent WHERE alert_key = ?", (alert_key,)).fetchone()
    return row is not None


def record_alert(connection: sqlite3.Connection, alert_key: str, category: str) -> None:
    """Record that an alert has been sent."""
    connection.execute(
        "INSERT OR IGNORE INTO alerts_sent (alert_key, category, sent_at) VALUES (?, ?, ?)",
        (alert_key, category, utc_now()),
    )


def record_collector_run(
    connection: sqlite3.Connection,
    collector_name: str,
    started_at: str,
    completed_at: str,
    success: bool,
    item_count: int,
    error_summary: str = "",
) -> None:
    """Persist collector run telemetry."""
    connection.execute(
        """
        INSERT INTO collector_runs (
            collector_name, started_at, completed_at, success, item_count, error_summary
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (collector_name, started_at, completed_at, int(success), item_count, error_summary),
    )


def record_ai_rankings_snapshot(
    connection: sqlite3.Connection,
    team_number: str,
    payload: dict[str, Any],
) -> None:
    """Persist the latest synthesized AI rankings snapshot for one team."""
    connection.execute(
        """
        INSERT OR REPLACE INTO ai_rankings_snapshots (
            team_number, generated_at, source_snapshot_at, source_type, confidence, headline, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            team_number,
            payload.get("generated_at") or utc_now(),
            payload.get("source_snapshot_at"),
            payload.get("source_type") or "unknown",
            payload.get("confidence", {}).get("level", "unknown"),
            payload.get("headline") or "No AI rankings summary available.",
            to_json(payload),
        ),
    )


def get_latest_ai_rankings(connection: sqlite3.Connection, team_number: str = "7157B") -> dict[str, Any] | None:
    """Return the stored latest AI rankings snapshot for one team."""
    row = connection.execute(
        """
        SELECT raw_json
        FROM ai_rankings_snapshots
        WHERE team_number = ?
        LIMIT 1
        """,
        (team_number,),
    ).fetchone()
    if row is None or row["raw_json"] in (None, ""):
        return None
    return json.loads(str(row["raw_json"]))


def get_latest_snapshot(connection: sqlite3.Connection) -> dict[str, Any] | None:
    """Return the latest focal team competition snapshot."""
    row = connection.execute(
        "SELECT * FROM competition_snapshots ORDER BY fetched_at DESC, id DESC LIMIT 1"
    ).fetchone()
    return row_to_dict(row)


def get_previous_snapshot(connection: sqlite3.Connection) -> dict[str, Any] | None:
    """Return the snapshot before the latest focal team snapshot."""
    row = connection.execute(
        "SELECT * FROM competition_snapshots ORDER BY fetched_at DESC, id DESC LIMIT 1 OFFSET 1"
    ).fetchone()
    return row_to_dict(row)


def get_recent_matches(connection: sqlite3.Connection, *, status: str, limit: int = 10) -> list[dict[str, Any]]:
    """Return recent focal team match rows filtered by status."""
    rows = connection.execute(
        """
        SELECT * FROM matches
        WHERE status = ?
        ORDER BY COALESCE(completed_time, scheduled_time, updated_at) DESC
        LIMIT ?
        """,
        (status, limit),
    ).fetchall()
    hydrated: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        try:
            raw = json.loads(str(row_dict.get("raw_json") or "{}"))
        except json.JSONDecodeError:
            raw = {}
        row_dict["source"] = raw.get("source")
        row_dict["source_state"] = raw.get("source_state")
        row_dict["result_tab"] = raw.get("result_tab")
        hydrated.append(row_dict)
    if status == "scheduled":
        hydrated = [row for row in hydrated if _is_future_match(row)]
    return hydrated


def _split_opponents(opponent_text: str | None) -> list[str]:
    """Split a stored opponent field into team numbers."""
    if not opponent_text:
        return []
    return [item.strip() for item in str(opponent_text).split(",") if item.strip() and item.strip() != "TBD"]


def _parse_match_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-ish scheduled or completed timestamp."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _is_future_match(match: dict[str, Any], *, grace_minutes: int = 5) -> bool:
    """Return true when a scheduled row is still plausibly in the future."""
    scheduled = _parse_match_datetime(match.get("scheduled_time"))
    if scheduled is None:
        return True
    if scheduled.tzinfo is None:
        scheduled = scheduled.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return scheduled >= now or (now - scheduled).total_seconds() <= grace_minutes * 60


def _match_sequence_value(match: dict[str, Any] | None) -> int | None:
    """Return the numeric portion of a round label or match key when available."""
    if not match:
        return None
    label = str(match.get("round_label") or match.get("match_key") or "").strip()
    digits = "".join(ch for ch in label if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _scheduled_sort_key(match: dict[str, Any]) -> tuple[int, int, str, str]:
    """Sort scheduled qualification rows by sequence first, then time."""
    sequence = _match_sequence_value(match)
    scheduled = str(match.get("scheduled_time") or "")
    label = str(match.get("round_label") or match.get("match_key") or "")
    return (0 if sequence is not None else 1, sequence if sequence is not None else 999999, scheduled, label)


def _latest_rankings_map(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Return the latest division rankings indexed by team number."""
    rows = get_latest_division_rankings(connection, limit=300)
    return {str(row["team_number"]): row for row in rows}


def _latest_power_map(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Return the latest power rankings indexed by team number."""
    rows = get_latest_power_rankings(connection, limit=300)
    return {str(row["team_number"]): row for row in rows}


def _latest_skills_map(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Return the latest skills rows indexed by team number."""
    rows = get_latest_skills(connection, limit=300)
    return {str(row["team_number"]): row for row in rows}


def _enrich_match_row(
    match: dict[str, Any] | None,
    rankings_map: dict[str, dict[str, Any]],
    power_map: dict[str, dict[str, Any]],
    skills_map: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Attach opponent ranking context to one focal-team match row."""
    if not match:
        return None
    opponents = _split_opponents(match.get("opponent"))
    opponent_rows: list[dict[str, Any]] = []
    for team_number in opponents:
        ranking = rankings_map.get(team_number) or {}
        power = power_map.get(team_number) or {}
        skill = skills_map.get(team_number) or {}
        opponent_rows.append(
            {
                "team_number": team_number,
                "official_rank": ranking.get("rank"),
                "record_text": ranking.get("record_text"),
                "wp": ranking.get("wp"),
                "ap": ranking.get("ap"),
                "sp": ranking.get("sp"),
                "power_rank": power.get("power_rank"),
                "opr": power.get("opr"),
                "dpr": power.get("dpr"),
                "ccwm": power.get("ccwm"),
                "skills_total": skill.get("total_score"),
            }
        )
    average_official = None
    official_values = [float(item["official_rank"]) for item in opponent_rows if item.get("official_rank") is not None]
    if official_values:
        average_official = round(sum(official_values) / len(official_values), 2)
    average_power = None
    power_values = [float(item["power_rank"]) for item in opponent_rows if item.get("power_rank") is not None]
    if power_values:
        average_power = round(sum(power_values) / len(power_values), 2)
    return {
        **match,
        "opponent_teams": opponents,
        "opponent_rows": opponent_rows,
        "opponent_average_official_rank": average_official,
        "opponent_average_power_rank": average_power,
    }


def _fallback_match_from_division(
    connection: sqlite3.Connection,
    *,
    team_number: str,
    status: str,
    order_expression: str,
) -> dict[str, Any] | None:
    """Build a focal-team-style match row from division matches when needed."""
    row = connection.execute(
        f"""
        SELECT *
        FROM division_matches
        WHERE match_key IN (
            SELECT match_key
            FROM match_participation
            WHERE team_number = ? AND status = ?
        )
        ORDER BY {order_expression}
        LIMIT 1
        """,
        (team_number, status),
    ).fetchone()
    if row is None:
        return None
    row_dict = dict(row)
    try:
        raw = json.loads(str(row_dict.get("raw_json") or "{}"))
    except json.JSONDecodeError:
        raw = {}
    red_teams = json.loads(row_dict.get("red_teams_json") or "[]")
    blue_teams = json.loads(row_dict.get("blue_teams_json") or "[]")
    if team_number in red_teams:
        alliance = "red"
        opponents = blue_teams
        score_for = row_dict.get("red_score")
        score_against = row_dict.get("blue_score")
    elif team_number in blue_teams:
        alliance = "blue"
        opponents = red_teams
        score_for = row_dict.get("blue_score")
        score_against = row_dict.get("red_score")
    else:
        return None
    return {
        "match_key": row_dict["match_key"],
        "event_sku": row_dict["event_sku"],
        "division_name": row_dict["division_name"],
        "team_number": team_number,
        "match_type": row_dict.get("match_type"),
        "round_label": row_dict.get("round_label"),
        "instance": row_dict.get("instance"),
        "status": row_dict["status"],
        "scheduled_time": row_dict.get("scheduled_time"),
        "completed_time": row_dict.get("completed_time"),
        "field_id": row_dict.get("field_id"),
        "field_name": row_dict.get("field_name"),
        "alliance": alliance,
        "opponent": ", ".join(opponents) if opponents else "TBD",
        "score_for": score_for,
        "score_against": score_against,
        "source": raw.get("source"),
        "source_state": raw.get("source_state"),
        "result_tab": raw.get("result_tab"),
    }


def get_match_intelligence(connection: sqlite3.Connection, team_number: str = "7157B") -> dict[str, Any]:
    """Return enriched match context for the focal team."""
    rankings_map = _latest_rankings_map(connection)
    power_map = _latest_power_map(connection)
    skills_map = _latest_skills_map(connection)
    last_match = connection.execute(
        """
        SELECT *
        FROM matches
        WHERE status = 'completed'
        ORDER BY COALESCE(completed_time, updated_at) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if last_match is None:
        last_match = _fallback_match_from_division(
            connection,
            team_number=team_number,
            status="completed",
            order_expression="COALESCE(completed_time, updated_at) DESC, id DESC",
        )
    upcoming_matchups = get_upcoming_matchups(connection, team_number=team_number, limit=1)
    enriched_next = upcoming_matchups[0] if upcoming_matchups else None
    enriched_last = _enrich_match_row(row_to_dict(last_match) if isinstance(last_match, sqlite3.Row) else last_match, rankings_map, power_map, skills_map)
    return {
        "next_match": enriched_next,
        "last_match": enriched_last,
    }


def get_upcoming_matchups(connection: sqlite3.Connection, team_number: str = "7157B", limit: int = 5) -> list[dict[str, Any]]:
    """Return the next known focal-team matchups with partner and opponent ranking context."""
    rankings_map = _latest_rankings_map(connection)
    power_map = _latest_power_map(connection)
    skills_map = _latest_skills_map(connection)
    rows = connection.execute(
        """
        SELECT *
        FROM division_matches
        WHERE status = 'scheduled'
        ORDER BY COALESCE(scheduled_time, updated_at) ASC, id ASC
        """
    ).fetchall()
    upcoming: list[dict[str, Any]] = []
    for row in rows:
        try:
            raw = json.loads(str(row["raw_json"] or "{}"))
        except json.JSONDecodeError:
            raw = {}
        base = _fallback_match_from_division(
            connection,
            team_number=team_number,
            status="scheduled",
            order_expression="COALESCE(scheduled_time, updated_at) ASC, id ASC",
        )
        if base is None or base["match_key"] != row["match_key"]:
            red_teams, blue_teams = _load_team_lists(row)
            if team_number in red_teams:
                alliance = "red"
                partners = [team for team in red_teams if team != team_number]
                opponents = blue_teams
            elif team_number in blue_teams:
                alliance = "blue"
                partners = [team for team in blue_teams if team != team_number]
                opponents = red_teams
            else:
                continue
            base = {
                "match_key": row["match_key"],
                "event_sku": row["event_sku"],
                "division_name": row["division_name"],
                "team_number": team_number,
                "match_type": row["match_type"],
                "round_label": row["round_label"],
                "instance": row["instance"],
                "status": row["status"],
                "scheduled_time": row["scheduled_time"],
                "completed_time": row["completed_time"],
                "field_id": row["field_id"],
                "field_name": row["field_name"],
                "alliance": alliance,
                "partner_teams": partners,
                "opponent": ", ".join(opponents) if opponents else "TBD",
                "source": raw.get("source"),
                "source_state": raw.get("source_state"),
                "result_tab": raw.get("result_tab"),
            }
        enriched = _enrich_match_row(base, rankings_map, power_map, skills_map)
        if not enriched:
            continue
        if not _is_future_match(enriched):
            continue
        partner_rows: list[dict[str, Any]] = []
        partner_teams = list(base.get("partner_teams") or [])
        for partner in partner_teams:
            ranking = rankings_map.get(partner) or {}
            power = power_map.get(partner) or {}
            skill = skills_map.get(partner) or {}
            partner_rows.append(
                {
                    "team_number": partner,
                    "official_rank": ranking.get("rank"),
                    "record_text": ranking.get("record_text"),
                    "power_rank": power.get("power_rank"),
                    "skills_total": skill.get("total_score"),
                    "opr": power.get("opr"),
                }
            )
        partner_official_values = [float(item["official_rank"]) for item in partner_rows if item.get("official_rank") is not None]
        partner_power_values = [float(item["power_rank"]) for item in partner_rows if item.get("power_rank") is not None]
        partner_average_official = round(sum(partner_official_values) / len(partner_official_values), 2) if partner_official_values else None
        partner_average_power = round(sum(partner_power_values) / len(partner_power_values), 2) if partner_power_values else None
        opponent_pressure = enriched.get("opponent_average_power_rank")
        if opponent_pressure is not None and float(opponent_pressure) <= 15:
            call = "High-pressure draw against strong opposition."
        elif opponent_pressure is not None and float(opponent_pressure) <= 35:
            call = "Competitive swing match with real movement potential."
        elif opponent_pressure is not None:
            call = "Manageable matchup if execution is clean."
        else:
            call = "Incomplete opponent ranking context."
        upcoming.append(
            {
                **enriched,
                "partner_teams": partner_teams,
                "partner_rows": partner_rows,
                "partner_average_official_rank": partner_average_official,
                "partner_average_power_rank": partner_average_power,
                "matchup_call": call,
            }
        )
    upcoming.sort(key=_scheduled_sort_key)
    return upcoming[:limit]


def get_alliance_impact(connection: sqlite3.Connection, team_number: str, limit: int = 6) -> dict[str, Any]:
    """Summarize partner and opponent impact around the focal team."""
    rankings_map = _latest_rankings_map(connection)
    power_map = _latest_power_map(connection)
    skills_map = _latest_skills_map(connection)
    rows = connection.execute(
        """
        SELECT *
        FROM match_participation
        WHERE team_number = ? AND status = 'completed'
        ORDER BY COALESCE(completed_time, match_key) DESC
        LIMIT 50
        """,
        (team_number,),
    ).fetchall()
    if not rows:
        return {
            "partner_rows": [],
            "opponent_rows": [],
            "partner_average_margin": None,
            "opponent_average_margin": None,
            "completed_matches": 0,
        }

    partner_stats: dict[str, dict[str, Any]] = {}
    opponent_stats: dict[str, dict[str, Any]] = {}
    total_margin = 0.0
    for row in rows:
        margin = float(row["margin"] or 0.0)
        total_margin += margin
        partners = json.loads(row["partner_teams_json"] or "[]")
        opponents = json.loads(row["opponent_teams_json"] or "[]")
        for partner in partners:
            stat = partner_stats.setdefault(
                partner,
                {"team_number": partner, "matches": 0, "total_margin": 0.0},
            )
            stat["matches"] += 1
            stat["total_margin"] += margin
        for opponent in opponents:
            stat = opponent_stats.setdefault(
                opponent,
                {"team_number": opponent, "matches": 0, "total_margin": 0.0},
            )
            stat["matches"] += 1
            stat["total_margin"] += margin

    def _decorate(stats_map: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []
        for team, stat in stats_map.items():
            ranking = rankings_map.get(team) or {}
            power = power_map.get(team) or {}
            skill = skills_map.get(team) or {}
            matches = int(stat["matches"])
            avg_margin = stat["total_margin"] / matches if matches else 0.0
            rows_out.append(
                {
                    "team_number": team,
                    "matches": matches,
                    "average_margin": round(avg_margin, 2),
                    "official_rank": ranking.get("rank"),
                    "power_rank": power.get("power_rank"),
                    "skills_total": skill.get("total_score"),
                    "opr": power.get("opr"),
                }
            )
        rows_out.sort(key=lambda item: (-float(item["average_margin"]), item["team_number"]))
        return rows_out[:limit]

    completed_matches = len(rows)
    return {
        "partner_rows": _decorate(partner_stats),
        "opponent_rows": sorted(
            _decorate(opponent_stats),
            key=lambda item: (float(item["average_margin"]), item["team_number"]),
        )[:limit],
        "partner_average_margin": round(total_margin / completed_matches, 2) if completed_matches else None,
        "completed_matches": completed_matches,
    }


def get_swing_matches(connection: sqlite3.Connection, team_number: str, limit: int = 6) -> list[dict[str, Any]]:
    """Return upcoming matches most likely to affect the focal team's trajectory."""
    focal_snapshot = get_latest_snapshot(connection)
    focal_power = get_latest_team_power(connection, team_number)
    if not focal_snapshot:
        return []
    focal_official = int(focal_snapshot["rank"]) if focal_snapshot.get("rank") is not None else None
    focal_power_rank = int(focal_power["power_rank"]) if focal_power and focal_power.get("power_rank") is not None else None
    rankings_map = _latest_rankings_map(connection)
    power_map = _latest_power_map(connection)
    skills_map = _latest_skills_map(connection)
    rows = get_recent_matches(connection, status="scheduled", limit=20)
    swing_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched = _enrich_match_row(row, rankings_map, power_map, skills_map)
        if not enriched:
            continue
        official_values = [
            abs(int(item["official_rank"]) - focal_official)
            for item in enriched["opponent_rows"]
            if item.get("official_rank") is not None and focal_official is not None
        ]
        power_values = [
            abs(int(item["power_rank"]) - focal_power_rank)
            for item in enriched["opponent_rows"]
            if item.get("power_rank") is not None and focal_power_rank is not None
        ]
        avg_opr = 0.0
        opr_values = [float(item["opr"]) for item in enriched["opponent_rows"] if item.get("opr") is not None]
        if opr_values:
            avg_opr = sum(opr_values) / len(opr_values)
        closeness_official = 20.0 - (sum(official_values) / len(official_values) if official_values else 20.0)
        closeness_power = 20.0 - (sum(power_values) / len(power_values) if power_values else 20.0)
        swing_score = max(0.0, closeness_official) + max(0.0, closeness_power) + min(avg_opr, 15.0)
        upside_score = max(0.0, closeness_official) + max(0.0, closeness_power)
        pressure_score = min(avg_opr, 15.0)
        if pressure_score >= 10:
            risk_level = "High"
        elif pressure_score >= 5:
            risk_level = "Moderate"
        else:
            risk_level = "Low"
        swing_rows.append(
            {
                **enriched,
                "swing_score": round(swing_score, 2),
                "upside_score": round(upside_score, 2),
                "pressure_score": round(pressure_score, 2),
                "risk_level": risk_level,
                "ai_call": (
                    f"{risk_level}-risk swing spot with upside score {round(upside_score, 2)} "
                    f"and opponent pressure {round(pressure_score, 2)}."
                ),
            }
        )
    swing_rows.sort(key=lambda item: (-float(item["swing_score"]), item.get("scheduled_time") or "", item["match_key"]))
    return swing_rows[:limit]


def get_recent_media(connection: sqlite3.Connection, limit: int = 25) -> list[dict[str, Any]]:
    """Return recent media mentions."""
    rows = connection.execute(
        "SELECT * FROM media_items ORDER BY discovered_at DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_snapshot_history(connection: sqlite3.Connection, limit: int = 25) -> list[dict[str, Any]]:
    """Return recent focal team competition snapshots."""
    rows = connection.execute(
        "SELECT * FROM competition_snapshots ORDER BY fetched_at DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def _build_sparkline(values: list[float]) -> dict[str, Any]:
    """Build a compact SVG sparkline payload."""
    if not values:
        return {"points": "", "min": None, "max": None, "latest": None, "count": 0}
    low = min(values)
    high = max(values)
    width = 100.0
    height = 28.0
    x_step = width / max(len(values) - 1, 1)
    span = high - low
    coords: list[str] = []
    for index, value in enumerate(values):
        x = round(index * x_step, 2)
        if math.isclose(span, 0.0):
            y = height / 2.0
        else:
            y = round(height - (((value - low) / span) * height), 2)
        coords.append(f"{x},{y}")
    return {
        "points": " ".join(coords),
        "min": low,
        "max": high,
        "latest": values[-1],
        "count": len(values),
    }


def get_collector_history(connection: sqlite3.Connection, limit: int = 25) -> list[dict[str, Any]]:
    """Return recent collector runs."""
    rows = connection.execute(
        "SELECT * FROM collector_runs ORDER BY completed_at DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_latest_collector_run(connection: sqlite3.Connection, collector_name: str) -> dict[str, Any] | None:
    """Return the latest run for a specific collector."""
    row = connection.execute(
        """
        SELECT * FROM collector_runs
        WHERE collector_name = ?
        ORDER BY completed_at DESC, id DESC
        LIMIT 1
        """,
        (collector_name,),
    ).fetchone()
    return row_to_dict(row)


def get_latest_rankings_collector_run(connection: sqlite3.Connection) -> dict[str, Any] | None:
    """Return the newest rankings-relevant collector run."""
    row = connection.execute(
        """
        SELECT *
        FROM collector_runs
        WHERE collector_name IN ('robotevents', 'vexvia_local', 'manual_import_standings')
        ORDER BY completed_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return row_to_dict(row)


def get_latest_division_snapshot_at(connection: sqlite3.Connection) -> str | None:
    """Return the latest division snapshot timestamp."""
    row = connection.execute(
        "SELECT snapshot_at FROM division_rankings_snapshots ORDER BY snapshot_at DESC LIMIT 1"
    ).fetchone()
    return str(row["snapshot_at"]) if row else None


def get_latest_skills_snapshot_at(connection: sqlite3.Connection) -> str | None:
    """Return the latest skills snapshot timestamp."""
    row = connection.execute(
        "SELECT snapshot_at FROM skills_snapshots ORDER BY snapshot_at DESC LIMIT 1"
    ).fetchone()
    return str(row["snapshot_at"]) if row else None


def get_latest_power_snapshot_at(connection: sqlite3.Connection) -> str | None:
    """Return the latest derived-metrics snapshot timestamp."""
    row = connection.execute(
        "SELECT snapshot_at FROM derived_metrics_snapshots ORDER BY snapshot_at DESC LIMIT 1"
    ).fetchone()
    return str(row["snapshot_at"]) if row else None


def get_previous_division_snapshot_at(connection: sqlite3.Connection) -> str | None:
    """Return the previous division snapshot timestamp."""
    row = connection.execute(
        "SELECT snapshot_at FROM division_rankings_snapshots GROUP BY snapshot_at ORDER BY snapshot_at DESC LIMIT 1 OFFSET 1"
    ).fetchone()
    return str(row["snapshot_at"]) if row else None


def get_latest_division_rankings(connection: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Return the latest official division standings."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return []
    rows = connection.execute(
        """
        SELECT * FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        ORDER BY rank ASC, team_number ASC
        LIMIT ?
        """,
        (snapshot_at, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def get_latest_division_snapshot_source(connection: sqlite3.Connection) -> str | None:
    """Return the source used for the latest division standings snapshot."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT json_extract(raw_json, '$.source') AS source
        FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        LIMIT 1
        """,
        (snapshot_at,),
    ).fetchone()
    if not row:
        return None
    return str(row["source"]) if row["source"] not in (None, "") else None


def get_latest_division_source_state(connection: sqlite3.Connection) -> str | None:
    """Return the source-state label for the latest standings snapshot."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT json_extract(raw_json, '$.source_state') AS source_state
        FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        LIMIT 1
        """,
        (snapshot_at,),
    ).fetchone()
    if not row:
        return None
    return str(row["source_state"]) if row["source_state"] not in (None, "") else None


def get_latest_result_tabs(connection: sqlite3.Connection) -> dict[str, list[str]]:
    """Return result-tab coverage inferred from the latest snapshots."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return {"standings": [], "skills": [], "division_matches": []}

    base_row = connection.execute(
        """
        SELECT event_sku, division_name
        FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        LIMIT 1
        """,
        (snapshot_at,),
    ).fetchone()
    if not base_row:
        return {"standings": [], "skills": [], "division_matches": []}

    def _tabs(query: str, params: tuple[Any, ...]) -> list[str]:
        rows = connection.execute(query, params).fetchall()
        return sorted(
            {
                str(row["result_tab"])
                for row in rows
                if row["result_tab"] not in (None, "", "null")
            }
        )

    standings_tabs = _tabs(
        """
        SELECT json_extract(raw_json, '$.result_tab') AS result_tab
        FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        """,
        (snapshot_at,),
    )
    skills_tabs = _tabs(
        """
        SELECT json_extract(raw_json, '$.result_tab') AS result_tab
        FROM skills_snapshots
        WHERE snapshot_at = ?
        """,
        (snapshot_at,),
    )
    division_match_tabs = _tabs(
        """
        SELECT json_extract(raw_json, '$.result_tab') AS result_tab
        FROM division_matches
        WHERE event_sku = ?
        AND division_name = ?
        """,
        (base_row["event_sku"], base_row["division_name"]),
    )
    return {
        "standings": standings_tabs,
        "skills": skills_tabs,
        "division_matches": division_match_tabs,
    }


def get_latest_division_source_updated_at(connection: sqlite3.Connection) -> str | None:
    """Return the source freshness timestamp from the latest standings snapshot."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT json_extract(raw_json, '$.source_updated_at') AS source_updated_at
        FROM division_rankings_snapshots
        WHERE snapshot_at = ?
        LIMIT 1
        """,
        (snapshot_at,),
    ).fetchone()
    if not row:
        return None
    return str(row["source_updated_at"]) if row["source_updated_at"] not in (None, "") else None


def get_latest_skills(connection: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Return the latest division skills standings."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return []
    rows = connection.execute(
        """
        SELECT * FROM skills_snapshots
        WHERE snapshot_at = ?
        ORDER BY total_score DESC, team_number ASC
        LIMIT ?
        """,
        (snapshot_at, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def get_latest_power_rankings(connection: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Return the latest derived power rankings."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return []
    rows = connection.execute(
        """
        SELECT * FROM derived_metrics_snapshots
        WHERE snapshot_at = ?
        ORDER BY power_rank ASC, team_number ASC
        LIMIT ?
        """,
        (snapshot_at, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def get_latest_team_skill(connection: sqlite3.Connection, team_number: str) -> dict[str, Any] | None:
    """Return the latest skills row for one team."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT * FROM skills_snapshots
        WHERE snapshot_at = ? AND team_number = ?
        LIMIT 1
        """,
        (snapshot_at, team_number),
    ).fetchone()
    return row_to_dict(row)


def get_latest_team_power(connection: sqlite3.Connection, team_number: str) -> dict[str, Any] | None:
    """Return the latest power ranking row for one team."""
    snapshot_at = get_latest_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT * FROM derived_metrics_snapshots
        WHERE snapshot_at = ? AND team_number = ?
        LIMIT 1
        """,
        (snapshot_at, team_number),
    ).fetchone()
    return row_to_dict(row)


def get_previous_team_power(connection: sqlite3.Connection, team_number: str) -> dict[str, Any] | None:
    """Return the previous power ranking row for one team."""
    snapshot_at = get_previous_division_snapshot_at(connection)
    if not snapshot_at:
        return None
    row = connection.execute(
        """
        SELECT * FROM derived_metrics_snapshots
        WHERE snapshot_at = ? AND team_number = ?
        LIMIT 1
        """,
        (snapshot_at, team_number),
    ).fetchone()
    return row_to_dict(row)


def get_biggest_movers(connection: sqlite3.Connection, limit: int = 10) -> list[dict[str, Any]]:
    """Return the biggest power rank movers between the last two snapshots."""
    latest_at = get_latest_division_snapshot_at(connection)
    previous_at = get_previous_division_snapshot_at(connection)
    if not latest_at or not previous_at:
        return []
    rows = connection.execute(
        """
        SELECT
            latest.team_number,
            latest.power_rank AS current_power_rank,
            previous.power_rank AS previous_power_rank,
            previous.power_rank - latest.power_rank AS movement
        FROM derived_metrics_snapshots AS latest
        JOIN derived_metrics_snapshots AS previous
          ON previous.team_number = latest.team_number
         AND previous.snapshot_at = ?
        WHERE latest.snapshot_at = ?
        ORDER BY ABS(previous.power_rank - latest.power_rank) DESC, latest.team_number ASC
        LIMIT ?
        """,
        (previous_at, latest_at, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def get_threat_list(connection: sqlite3.Connection, team_number: str, limit: int = 8) -> list[dict[str, Any]]:
    """Return nearby/scary teams relative to the focal team."""
    focal_snapshot = get_latest_snapshot(connection)
    focal_power = get_latest_team_power(connection, team_number)
    focal_skill = get_latest_team_skill(connection, team_number)
    division_rankings = get_latest_division_rankings(connection, limit=300)
    power_rankings = get_latest_power_rankings(connection, limit=300)
    skills_rankings = get_latest_skills(connection, limit=300)
    if not division_rankings:
        return []

    focal_official_rank = int(focal_snapshot["rank"]) if focal_snapshot and focal_snapshot.get("rank") is not None else None
    focal_power_rank = int(focal_power["power_rank"]) if focal_power and focal_power.get("power_rank") is not None else None
    focal_skills_total = float(focal_skill["total_score"]) if focal_skill and focal_skill.get("total_score") is not None else 0.0

    power_map = {str(row["team_number"]): row for row in power_rankings}
    skills_map = {str(row["team_number"]): row for row in skills_rankings}

    threats: list[dict[str, Any]] = []
    for row in division_rankings:
        candidate = dict(row)
        candidate_team = str(candidate["team_number"])
        if candidate_team == team_number:
            continue
        power_row = power_map.get(candidate_team) or {}
        skill_row = skills_map.get(candidate_team) or {}
        official_rank = int(candidate["rank"]) if candidate.get("rank") is not None else 999
        power_rank = int(power_row["power_rank"]) if power_row.get("power_rank") is not None else 999
        skills_total = float(skill_row["total_score"]) if skill_row.get("total_score") is not None else 0.0
        opr = float(power_row["opr"]) if power_row.get("opr") is not None else 0.0
        official_gap = abs(official_rank - focal_official_rank) if focal_official_rank is not None else 50
        power_gap = abs(power_rank - focal_power_rank) if focal_power_rank is not None else 50
        skills_edge = max(0.0, skills_total - focal_skills_total)
        official_pressure = max(0.0, 40.0 - official_gap)
        power_pressure = max(0.0, 30.0 - power_gap)
        skills_pressure = min(skills_edge / 5.0, 20.0)
        scoring_pressure = min(opr, 10.0)
        threat_score = official_pressure + power_pressure + skills_pressure + scoring_pressure
        if threat_score >= 65:
            threat_level = "Critical"
        elif threat_score >= 45:
            threat_level = "High"
        elif threat_score >= 25:
            threat_level = "Moderate"
        else:
            threat_level = "Watch"
        threats.append(
            {
                "team_number": candidate_team,
                "official_rank": candidate.get("rank"),
                "power_rank": power_row.get("power_rank"),
                "record_text": candidate.get("record_text"),
                "skills_total": skill_row.get("total_score"),
                "opr": power_row.get("opr"),
                "ccwm": power_row.get("ccwm"),
                "official_gap": official_gap,
                "power_gap": power_gap,
                "skills_edge": round(skills_edge, 2),
                "official_pressure": round(official_pressure, 2),
                "power_pressure": round(power_pressure, 2),
                "skills_pressure": round(skills_pressure, 2),
                "scoring_pressure": round(scoring_pressure, 2),
                "threat_level": threat_level,
                "threat_score": round(threat_score, 2),
            }
        )
    threats.sort(
        key=lambda item: (
            -float(item["threat_score"]),
            int(item["official_gap"]),
            int(item["power_gap"]),
            item["team_number"],
        )
    )
    return threats[:limit]


def get_team_rank_trend(connection: sqlite3.Connection, team_number: str, limit: int = 12) -> dict[str, Any]:
    """Return recent official-rank trend data for one team."""
    rows = connection.execute(
        """
        SELECT fetched_at AS timestamp, rank
        FROM competition_snapshots
        WHERE team_number = ? AND rank IS NOT NULL
        ORDER BY fetched_at DESC, id DESC
        LIMIT ?
        """,
        (team_number, limit),
    ).fetchall()
    ordered = list(reversed([dict(row) for row in rows]))
    values = [float(row["rank"]) for row in ordered]
    return {
        "label": "Official Rank",
        "direction": "improving"
        if len(values) >= 2 and values[-1] < values[0]
        else "declining"
        if len(values) >= 2 and values[-1] > values[0]
        else "flat",
        "history": ordered,
        "sparkline": _build_sparkline(values),
    }


def get_team_power_trend(connection: sqlite3.Connection, team_number: str, limit: int = 12) -> dict[str, Any]:
    """Return recent power-rank trend data for one team."""
    rows = connection.execute(
        """
        SELECT snapshot_at AS timestamp, power_rank
        FROM derived_metrics_snapshots
        WHERE team_number = ? AND power_rank IS NOT NULL
        ORDER BY snapshot_at DESC, id DESC
        LIMIT ?
        """,
        (team_number, limit),
    ).fetchall()
    ordered = list(reversed([dict(row) for row in rows]))
    values = [float(row["power_rank"]) for row in ordered]
    return {
        "label": "Power Rank",
        "direction": "improving"
        if len(values) >= 2 and values[-1] < values[0]
        else "declining"
        if len(values) >= 2 and values[-1] > values[0]
        else "flat",
        "history": ordered,
        "sparkline": _build_sparkline(values),
    }


def get_rankings_status(connection: sqlite3.Connection) -> dict[str, Any]:
    """Return operator-facing status for the rankings page."""
    latest_rankings_at = get_latest_division_snapshot_at(connection)
    latest_skills_at = get_latest_skills_snapshot_at(connection)
    latest_power_at = get_latest_power_snapshot_at(connection)
    latest_run = get_latest_rankings_collector_run(connection)
    rankings_count = 0
    skills_count = 0
    power_count = 0
    if latest_rankings_at:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM division_rankings_snapshots WHERE snapshot_at = ?",
            (latest_rankings_at,),
        ).fetchone()
        rankings_count = int(row["count"]) if row else 0
    if latest_skills_at:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM skills_snapshots WHERE snapshot_at = ?",
            (latest_skills_at,),
        ).fetchone()
        skills_count = int(row["count"]) if row else 0
    if latest_power_at:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM derived_metrics_snapshots WHERE snapshot_at = ?",
            (latest_power_at,),
        ).fetchone()
        power_count = int(row["count"]) if row else 0
    snapshot_source = get_latest_division_snapshot_source(connection)
    source_state = get_latest_division_source_state(connection)
    source_updated_at = get_latest_division_source_updated_at(connection)
    result_tabs = get_latest_result_tabs(connection)
    has_rankings = rankings_count > 0
    has_skills = skills_count > 0
    has_power = power_count > 0
    if not latest_rankings_at:
        empty_reason = "No rankings data has been collected yet."
    elif has_rankings and not has_skills and not has_power:
        empty_reason = "Official standings are available, but skills and derived metrics are still missing."
    elif has_rankings and has_skills and not has_power:
        empty_reason = "Official standings and skills are available, but power rankings are not ready yet."
    else:
        empty_reason = ""
    return {
        "latest_rankings_snapshot_at": latest_rankings_at,
        "latest_skills_snapshot_at": latest_skills_at,
        "latest_power_snapshot_at": latest_power_at,
        "rankings_count": rankings_count,
        "skills_count": skills_count,
        "power_count": power_count,
        "snapshot_source": snapshot_source or "",
        "source_state": source_state or "",
        "source_updated_at": source_updated_at or "",
        "result_tabs": result_tabs,
        "latest_collector_run": latest_run,
        "has_rankings": has_rankings,
        "has_skills": has_skills,
        "has_power": has_power,
        "empty_reason": empty_reason,
    }


def compute_rank_delta(latest: dict[str, Any] | None, previous: dict[str, Any] | None) -> dict[str, Any]:
    """Compute the latest official rank and record deltas."""
    rank_change: int | None = None
    direction = "no change"
    if latest and previous and latest.get("rank") is not None and previous.get("rank") is not None:
        rank_change = int(previous["rank"]) - int(latest["rank"])
        if rank_change > 0:
            direction = "up"
        elif rank_change < 0:
            direction = "down"
    return {
        "rank_change": rank_change,
        "rank_direction": direction,
        "record_changed": bool(latest and previous and latest.get("record_text") != previous.get("record_text")),
    }


def compute_power_rank_delta(latest: dict[str, Any] | None, previous: dict[str, Any] | None) -> dict[str, Any]:
    """Compute a power rank delta."""
    change: int | None = None
    direction = "no change"
    if latest and previous and latest.get("power_rank") is not None and previous.get("power_rank") is not None:
        change = int(previous["power_rank"]) - int(latest["power_rank"])
        if change > 0:
            direction = "up"
        elif change < 0:
            direction = "down"
    return {"power_rank_change": change, "power_rank_direction": direction}


def _load_team_lists(match_row: sqlite3.Row | dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return red and blue team lists from a division match row."""
    row = dict(match_row)
    return (
        json.loads(row.get("red_teams_json") or "[]"),
        json.loads(row.get("blue_teams_json") or "[]"),
    )


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    """Solve a linear system with Gaussian elimination."""
    size = len(vector)
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for pivot in range(size):
        best = max(range(pivot, size), key=lambda index: abs(augmented[index][pivot]))
        augmented[pivot], augmented[best] = augmented[best], augmented[pivot]
        pivot_value = augmented[pivot][pivot]
        if abs(pivot_value) < 1e-9:
            continue
        for column in range(pivot, size + 1):
            augmented[pivot][column] /= pivot_value
        for row_index in range(size):
            if row_index == pivot:
                continue
            factor = augmented[row_index][pivot]
            if abs(factor) < 1e-9:
                continue
            for column in range(pivot, size + 1):
                augmented[row_index][column] -= factor * augmented[pivot][column]
    return [augmented[index][size] for index in range(size)]


def _least_squares(rows: list[tuple[list[str], float]], teams: list[str]) -> dict[str, float]:
    """Solve a ridge-regularized least-squares problem for team metrics."""
    if not teams or not rows:
        return {team: 0.0 for team in teams}
    index = {team: position for position, team in enumerate(teams)}
    size = len(teams)
    ata = [[0.0 for _ in range(size)] for _ in range(size)]
    atb = [0.0 for _ in range(size)]
    for lineup, outcome in rows:
        counts = [0.0 for _ in range(size)]
        for team in lineup:
            if team in index:
                counts[index[team]] += 1.0
        for i in range(size):
            atb[i] += counts[i] * outcome
            for j in range(size):
                ata[i][j] += counts[i] * counts[j]
    for diagonal in range(size):
        ata[diagonal][diagonal] += 1e-6
    solved = _solve_linear_system(ata, atb)
    return {team: solved[index[team]] for team in teams}


def _normalize_metric(values: dict[str, float], *, invert: bool = False) -> dict[str, float]:
    """Normalize a metric into a 0-1 range."""
    if not values:
        return {}
    low = min(values.values())
    high = max(values.values())
    if math.isclose(low, high):
        return {team: 1.0 for team in values}
    normalized = {
        team: (value - low) / (high - low)
        for team, value in values.items()
    }
    if invert:
        return {team: 1.0 - value for team, value in normalized.items()}
    return normalized


def compute_and_store_derived_metrics(
    connection: sqlite3.Connection,
    *,
    snapshot_at: str,
    event_sku: str,
    division_name: str,
    recent_match_count: int,
    weights: dict[str, float],
) -> list[dict[str, Any]]:
    """Compute OPR/DPR/CCWM and a composite power ranking for the latest division state."""
    ranking_rows = connection.execute(
        """
        SELECT * FROM division_rankings_snapshots
        WHERE snapshot_at = ? AND event_sku = ? AND division_name = ?
        ORDER BY rank ASC
        """,
        (snapshot_at, event_sku, division_name),
    ).fetchall()
    if not ranking_rows:
        return []

    skills_rows = connection.execute(
        """
        SELECT * FROM skills_snapshots
        WHERE snapshot_at = ? AND event_sku = ? AND division_name = ?
        """,
        (snapshot_at, event_sku, division_name),
    ).fetchall()
    skills_map = {str(row["team_number"]): float(row["total_score"] or 0.0) for row in skills_rows}
    teams = [str(row["team_number"]) for row in ranking_rows]

    match_rows = connection.execute(
        """
        SELECT * FROM division_matches
        WHERE event_sku = ? AND division_name = ? AND status = 'completed'
        ORDER BY COALESCE(completed_time, updated_at) ASC
        """,
        (event_sku, division_name),
    ).fetchall()

    opr_inputs: list[tuple[list[str], float]] = []
    dpr_inputs: list[tuple[list[str], float]] = []
    ccwm_inputs: list[tuple[list[str], float]] = []
    recent_form_rows = connection.execute(
        """
        SELECT * FROM match_participation
        WHERE event_sku = ? AND division_name = ? AND status = 'completed'
        ORDER BY COALESCE(completed_time, match_key) DESC
        """,
        (event_sku, division_name),
    ).fetchall()
    form_map: dict[str, list[float]] = {}
    for row in recent_form_rows:
        team_number = str(row["team_number"])
        form_map.setdefault(team_number, [])
        if len(form_map[team_number]) < recent_match_count:
            form_map[team_number].append(float(row["margin"] or 0.0))

    for row in match_rows:
        red_teams, blue_teams = _load_team_lists(row)
        red_score = float(row["red_score"] or 0.0)
        blue_score = float(row["blue_score"] or 0.0)
        if red_teams:
            opr_inputs.append((red_teams, red_score))
            dpr_inputs.append((red_teams, blue_score))
            ccwm_inputs.append((red_teams, red_score - blue_score))
        if blue_teams:
            opr_inputs.append((blue_teams, blue_score))
            dpr_inputs.append((blue_teams, red_score))
            ccwm_inputs.append((blue_teams, blue_score - red_score))

    opr_map = _least_squares(opr_inputs, teams)
    dpr_map = _least_squares(dpr_inputs, teams)
    ccwm_map = _least_squares(ccwm_inputs, teams)
    form_score_map = {
        team: (sum(values) / len(values) if values else 0.0)
        for team, values in form_map.items()
    }
    for team in teams:
        form_score_map.setdefault(team, 0.0)
        skills_map.setdefault(team, 0.0)

    official_component = {
        str(row["team_number"]): float(len(teams) - int(row["rank"] or len(teams)) + 1)
        for row in ranking_rows
    }
    normalized_official = _normalize_metric(official_component)
    normalized_opr = _normalize_metric(opr_map)
    normalized_dpr = _normalize_metric(dpr_map, invert=True)
    normalized_ccwm = _normalize_metric(ccwm_map)
    normalized_skills = _normalize_metric(skills_map)
    normalized_form = _normalize_metric(form_score_map)

    metrics: list[dict[str, Any]] = []
    for row in ranking_rows:
        team = str(row["team_number"])
        composite = (
            weights["official"] * normalized_official.get(team, 0.0)
            + weights["opr"] * normalized_opr.get(team, 0.0)
            + weights["dpr"] * normalized_dpr.get(team, 0.0)
            + weights["ccwm"] * normalized_ccwm.get(team, 0.0)
            + weights["skills"] * normalized_skills.get(team, 0.0)
            + weights["form"] * normalized_form.get(team, 0.0)
        )
        metrics.append(
            {
                "snapshot_at": snapshot_at,
                "event_sku": event_sku,
                "division_name": division_name,
                "team_number": team,
                "official_rank": row["rank"],
                "skills_total": skills_map.get(team, 0.0),
                "opr": round(opr_map.get(team, 0.0), 3),
                "dpr": round(dpr_map.get(team, 0.0), 3),
                "ccwm": round(ccwm_map.get(team, 0.0), 3),
                "recent_form": round(form_score_map.get(team, 0.0), 3),
                "composite_score": round(composite, 6),
            }
        )

    metrics.sort(key=lambda item: (-float(item["composite_score"]), int(item["official_rank"] or 9999), item["team_number"]))
    for index, item in enumerate(metrics, start=1):
        item["power_rank"] = index
        connection.execute(
            """
            INSERT OR REPLACE INTO derived_metrics_snapshots (
                snapshot_at, event_sku, division_name, team_number, official_rank,
                skills_total, opr, dpr, ccwm, recent_form, composite_score,
                power_rank, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["snapshot_at"],
                item["event_sku"],
                item["division_name"],
                item["team_number"],
                item["official_rank"],
                item["skills_total"],
                item["opr"],
                item["dpr"],
                item["ccwm"],
                item["recent_form"],
                item["composite_score"],
                item["power_rank"],
                to_json(item),
            ),
        )
    return metrics


def build_dashboard_view(connection: sqlite3.Connection, team_number: str = "7157B") -> dict[str, Any]:
    """Collect the current database state for reporting and the GUI."""
    latest = get_latest_snapshot(connection)
    previous = get_previous_snapshot(connection)
    recent_completed = get_recent_matches(connection, status="completed", limit=10)
    upcoming = get_recent_matches(connection, status="scheduled", limit=10)
    media = get_recent_media(connection, limit=25)
    snapshots = get_snapshot_history(connection, limit=25)
    collector_runs = get_collector_history(connection, limit=25)
    latest_delta = compute_rank_delta(latest, previous)
    division_rankings = get_latest_division_rankings(connection, limit=200)
    skills = get_latest_skills(connection, limit=200)
    power_rankings = get_latest_power_rankings(connection, limit=200)
    team_skill = get_latest_team_skill(connection, team_number)
    team_power = get_latest_team_power(connection, team_number)
    previous_power = get_previous_team_power(connection, team_number)
    movers = get_biggest_movers(connection, limit=10)
    threat_list = get_threat_list(connection, team_number, limit=10)
    rank_trend = get_team_rank_trend(connection, team_number)
    power_trend = get_team_power_trend(connection, team_number)
    match_intelligence = get_match_intelligence(connection, team_number)
    alliance_impact = get_alliance_impact(connection, team_number, limit=8)
    swing_matches = get_swing_matches(connection, team_number, limit=8)
    upcoming_matchups = get_upcoming_matchups(connection, team_number, limit=5)
    matchup_summary = _build_matchup_summary(upcoming_matchups)
    rankings_status = get_rankings_status(connection)
    base_view = {
        "latest_snapshot": latest,
        "previous_snapshot": previous,
        "recent_completed_matches": recent_completed,
        "upcoming_matches": upcoming,
        "recent_media": media,
        "snapshot_history": snapshots,
        "collector_runs": collector_runs,
        "delta": latest_delta,
        "division_rankings": division_rankings,
        "skills_rankings": skills,
        "power_rankings": power_rankings,
        "team_skill": team_skill,
        "team_power": team_power,
        "power_delta": compute_power_rank_delta(team_power, previous_power),
        "biggest_movers": movers,
        "threat_list": threat_list,
        "rank_trend": rank_trend,
        "power_trend": power_trend,
        "match_intelligence": match_intelligence,
        "alliance_impact": alliance_impact,
        "swing_matches": swing_matches,
        "upcoming_matchups": upcoming_matchups,
        "matchup_summary": matchup_summary,
        "rankings_status": rankings_status,
    }
    analysis = build_analysis(base_view)
    base_view["analysis"] = analysis
    base_view["ai_rankings"] = get_latest_ai_rankings(connection, team_number)
    return base_view


def generate_ai_rankings_snapshot(connection: sqlite3.Connection, team_number: str = "7157B") -> dict[str, Any]:
    """Generate and persist the latest AI rankings snapshot for one team."""
    view = build_dashboard_view(connection, team_number)
    payload = build_ai_rankings(view)
    record_ai_rankings_snapshot(connection, team_number, payload)
    view["ai_rankings"] = payload
    return payload


def _build_matchup_summary(upcoming_matchups: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a compact summary for the upcoming matchup slate."""
    if not upcoming_matchups:
        return {
            "count": 0,
            "headline": "No known upcoming 7157B matchups are available in the local cache.",
        }
    next_item = upcoming_matchups[0]
    opponents = ", ".join(next_item.get("opponent_teams") or ["TBD"])
    return {
        "count": len(upcoming_matchups),
        "headline": (
            f"Next {len(upcoming_matchups)} known matchups loaded. "
            f"Nearest is {next_item.get('round_label') or next_item.get('match_key')} against {opponents} "
            f"at {next_item.get('scheduled_time') or 'TBD'} on {next_item.get('field_name') or 'TBD'}."
        ),
    }
