"""Local VEX Via cache collector for autonomous standings ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import logging
from pathlib import Path
import sqlite3
from typing import Any

from config import Settings
from collectors.robotevents import RobotEventsResult
from storage.db import utc_now

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _LocalPaths:
    """Resolved local cache paths for VEX Via."""

    event_db: Path
    skills_db: Path | None = None


class VexViaLocalCollector:
    """Read locally cached event data from the installed VEX Via macOS container."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _candidate_event_db_paths(self) -> list[Path]:
        """Return candidate event-db paths in preferred order."""
        candidates: list[Path] = []
        if self.settings.vexvia_event_db_path:
            candidates.append(self.settings.vexvia_event_db_path)
        db_name = f"{self.settings.event_sku.lower()}.db"
        if self.settings.vexvia_container_path:
            candidates.append(
                self.settings.vexvia_container_path
                / "Data"
                / "Library"
                / "Application Support"
                / "Databases"
                / db_name
            )
        containers_root = Path.home() / "Library" / "Containers"
        if containers_root.exists():
            candidates.extend(
                sorted(
                    containers_root.glob(f"*/Data/Library/Application Support/Databases/{db_name}"),
                    key=lambda path: path.stat().st_mtime if path.exists() else 0,
                    reverse=True,
                )
            )
        seen: set[Path] = set()
        unique: list[Path] = []
        for path in candidates:
            resolved = path.expanduser().resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            unique.append(resolved)
        return unique

    def _candidate_skills_db_paths(self, event_db: Path) -> list[Path]:
        """Return candidate skills-db paths."""
        candidates: list[Path] = []
        if self.settings.vexvia_skills_db_path:
            candidates.append(self.settings.vexvia_skills_db_path)
        sibling = event_db.parent / "v5rc-hs-skills.db"
        candidates.append(sibling)
        seen: set[Path] = set()
        unique: list[Path] = []
        for path in candidates:
            resolved = path.expanduser().resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            unique.append(resolved)
        return unique

    def _resolve_paths(self) -> _LocalPaths:
        """Resolve the VEX Via local database paths."""
        for event_db in self._candidate_event_db_paths():
            if not event_db.exists():
                continue
            skills_db = next((path for path in self._candidate_skills_db_paths(event_db) if path.exists()), None)
            return _LocalPaths(event_db=event_db, skills_db=skills_db)
        raise RuntimeError("No local VEX Via event database was found")

    def _connect(self, path: Path) -> sqlite3.Connection:
        """Open a read-only SQLite connection."""
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        return connection

    def _fetch_division_row(self, connection: sqlite3.Connection) -> sqlite3.Row:
        """Return the configured division row."""
        row = connection.execute(
            """
            SELECT *
            FROM divisions
            WHERE lower(name) = lower(?)
            LIMIT 1
            """,
            (self.settings.division_name,),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"Division '{self.settings.division_name}' not found in local VEX Via cache")
        return row

    def _fetch_last_modified(self, connection: sqlite3.Connection) -> str:
        """Return the app cache's last-modified timestamp when present."""
        row = connection.execute(
            """
            SELECT *
            FROM last_modified
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return ""
        row_map = dict(row)
        for candidate_key in ("value", "last_modified", "updated_at", "modified_at"):
            if candidate_key in row_map and row_map[candidate_key] not in (None, ""):
                raw_value = str(row_map[candidate_key]).strip()
                break
        else:
            raw_value = next(
                (str(value).strip() for key, value in row_map.items() if key != "id" and value not in (None, "")),
                "",
            )
        if not raw_value:
            return ""
        try:
            return parsedate_to_datetime(raw_value).isoformat()
        except Exception:
            return raw_value

    def _normalize_rankings(
        self,
        connection: sqlite3.Connection,
        *,
        division_id: int,
        division_name: str,
        fetched_at: str,
        source_updated_at: str,
    ) -> list[dict[str, Any]]:
        """Normalize local standings rows for the configured division."""
        round_row = connection.execute(
            """
            SELECT MAX(round) AS max_round
            FROM rankings
            WHERE division_id = ?
            """,
            (division_id,),
        ).fetchone()
        ranking_round = int(round_row["max_round"]) if round_row and round_row["max_round"] is not None else None
        if ranking_round is None:
            return []
        rows = connection.execute(
            """
            SELECT
                t.number AS team_number,
                r.rank,
                r.parm1 AS wins,
                r.parm2 AS losses,
                r.parm3 AS ties,
                r.parm4 AS wp,
                r.parm5 AS ap,
                r.parm6 AS sp,
                r.opr,
                r.dpr,
                r.ccwm
            FROM rankings AS r
            JOIN teams AS t ON t.id = r.team_id
            WHERE r.division_id = ? AND r.round = ?
            ORDER BY r.rank ASC, t.number ASC
            """,
            (division_id, ranking_round),
        ).fetchall()
        rankings: list[dict[str, Any]] = []
        for row in rows:
            wins = int(row["wins"]) if row["wins"] is not None else None
            losses = int(row["losses"]) if row["losses"] is not None else None
            ties = int(row["ties"]) if row["ties"] is not None else None
            rankings.append(
                {
                    "event_sku": self.settings.event_sku,
                    "event_name": self.settings.event_name_alias,
                    "division_name": division_name,
                    "team_number": str(row["team_number"]),
                    "team_name": "",
                    "organization": "",
                    "rank": int(row["rank"]) if row["rank"] is not None else None,
                    "wins": wins,
                    "losses": losses,
                    "ties": ties,
                    "wp": float(row["wp"]) if row["wp"] is not None else None,
                    "ap": float(row["ap"]) if row["ap"] is not None else None,
                    "sp": float(row["sp"]) if row["sp"] is not None else None,
                    "average_score": None,
                    "record_text": f"{wins}-{losses}-{ties}" if None not in (wins, losses, ties) else "Unknown",
                    "source": "vex_via_local",
                    "source_state": "live",
                    "result_tab": "vex_via_local_rankings",
                    "source_updated_at": source_updated_at,
                    "fetched_at": fetched_at,
                    "via_opr": float(row["opr"]) if row["opr"] is not None else None,
                    "via_dpr": float(row["dpr"]) if row["dpr"] is not None else None,
                    "via_ccwm": float(row["ccwm"]) if row["ccwm"] is not None else None,
                }
            )
        return rankings

    def _normalize_skills(
        self,
        event_connection: sqlite3.Connection,
        *,
        division_id: int,
        division_name: str,
        fetched_at: str,
        source_updated_at: str,
    ) -> list[dict[str, Any]]:
        """Normalize skills rows from the local cache."""
        rows = event_connection.execute(
            """
            SELECT
                t.number AS team_number,
                s.driver_score,
                s.prog_score,
                s.total_score
            FROM skills AS s
            JOIN teams AS t ON t.id = s.team_id
            WHERE t.division_num = ?
            ORDER BY COALESCE(s.total_score, 0) DESC, t.number ASC
            """,
            (division_id,),
        ).fetchall()
        skills: list[dict[str, Any]] = []
        for row in rows:
            skills.append(
                {
                    "event_sku": self.settings.event_sku,
                    "division_name": division_name,
                    "team_number": str(row["team_number"]),
                    "team_name": "",
                    "driver_score": float(row["driver_score"] or 0.0),
                    "programming_score": float(row["prog_score"] or 0.0),
                    "total_score": float(row["total_score"] or 0.0),
                    "source": "vex_via_local",
                    "source_state": "live",
                    "result_tab": "vex_via_local_skills",
                    "source_updated_at": source_updated_at,
                    "fetched_at": fetched_at,
                }
            )
        return skills

    def _normalize_division_matches(
        self,
        connection: sqlite3.Connection,
        *,
        division_id: int,
        division_name: str,
        fetched_at: str,
        source_updated_at: str,
    ) -> list[dict[str, Any]]:
        """Normalize division-wide matches from the local cache."""
        try:
            field_rows = connection.execute("SELECT id, name FROM fields").fetchall()
        except sqlite3.OperationalError:
            field_rows = []
        field_map = {int(row["id"]): str(row["name"]) for row in field_rows if row["id"] is not None}
        rows = connection.execute(
            """
            SELECT *
            FROM matches
            WHERE division_id = ?
            ORDER BY COALESCE(time_scheduled, 0) ASC, match ASC, id ASC
            """,
            (division_id,),
        ).fetchall()
        matches: list[dict[str, Any]] = []
        for row in rows:
            red_teams = [str(team) for team in (row["red_team1"], row["red_team2"]) if team]
            blue_teams = [str(team) for team in (row["blue_team1"], row["blue_team2"]) if team]
            session = str(row["session"] or "").strip()
            match_number = row["match"]
            field_id = int(row["field_id"]) if row["field_id"] is not None else None
            round_label = f"{session}{match_number}" if session and match_number is not None else str(row["id"])
            red_score = float(row["red_score"]) if row["red_score"] is not None else None
            blue_score = float(row["blue_score"]) if row["blue_score"] is not None else None
            status = "completed" if int(row["scored"] or 0) == 1 or (red_score is not None and blue_score is not None) else "scheduled"
            matches.append(
                {
                    "match_key": round_label,
                    "event_sku": self.settings.event_sku,
                    "division_name": division_name,
                    "match_type": session or "unknown",
                    "round_label": round_label,
                    "instance": row["instance"],
                    "status": status,
                    "scheduled_time": str(row["time_scheduled"]) if row["time_scheduled"] is not None else None,
                    "completed_time": str(row["time_scheduled"]) if status == "completed" and row["time_scheduled"] is not None else None,
                    "field_id": field_id,
                    "field_name": field_map.get(field_id),
                    "red_score": red_score,
                    "blue_score": blue_score,
                    "red_teams": red_teams,
                    "blue_teams": blue_teams,
                    "source": "vex_via_local",
                    "source_state": "live",
                    "result_tab": "vex_via_local_matches",
                    "source_updated_at": source_updated_at,
                    "fetched_at": fetched_at,
                }
            )
        return matches

    def _extract_focal_snapshot(
        self,
        rankings: list[dict[str, Any]],
        *,
        source_updated_at: str,
    ) -> dict[str, Any] | None:
        """Return the focal team snapshot from local rankings."""
        for item in rankings:
            if str(item.get("team_number", "")).lower() != self.settings.team_number.lower():
                continue
            return {
                **item,
                "school_name": self.settings.school_alias,
                "source_updated_at": source_updated_at,
            }
        return None

    def _extract_focal_matches(self, division_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract only focal-team matches from division-wide rows."""
        focal_matches: list[dict[str, Any]] = []
        for match in division_matches:
            if self.settings.team_number in match.get("red_teams", []):
                alliance = "red"
                opponents = match.get("blue_teams", [])
                score_for = match.get("red_score")
                score_against = match.get("blue_score")
            elif self.settings.team_number in match.get("blue_teams", []):
                alliance = "blue"
                opponents = match.get("red_teams", [])
                score_for = match.get("blue_score")
                score_against = match.get("red_score")
            else:
                continue
            focal_matches.append(
                {
                    "match_key": match["match_key"],
                    "event_sku": match["event_sku"],
                    "division_name": match["division_name"],
                    "team_number": self.settings.team_number,
                    "match_type": match.get("match_type"),
                    "round_label": match.get("round_label"),
                    "instance": match.get("instance"),
                    "status": match["status"],
                    "scheduled_time": match.get("scheduled_time"),
                    "completed_time": match.get("completed_time"),
                    "field_id": match.get("field_id"),
                    "field_name": match.get("field_name"),
                    "alliance": alliance,
                    "opponent": ", ".join(opponents) if opponents else "TBD",
                    "score_for": score_for,
                    "score_against": score_against,
                    "source": "vex_via_local",
                    "source_state": "live",
                    "result_tab": "vex_via_local_matches",
                    "source_updated_at": match.get("source_updated_at", ""),
                    "fetched_at": match.get("fetched_at"),
                }
            )
        return focal_matches

    def fetch(self) -> RobotEventsResult:
        """Fetch standings, skills, and matches from the local VEX Via cache."""
        warnings: list[str] = []
        paths = self._resolve_paths()
        fetched_at = utc_now()
        event_connection = self._connect(paths.event_db)
        try:
            division_row = self._fetch_division_row(event_connection)
            division_id = int(division_row["id"])
            division_name = str(division_row["name"])
            source_updated_at = self._fetch_last_modified(event_connection)
            division_rankings = self._normalize_rankings(
                event_connection,
                division_id=division_id,
                division_name=division_name,
                fetched_at=fetched_at,
                source_updated_at=source_updated_at,
            )
            skills = self._normalize_skills(
                event_connection,
                division_id=division_id,
                division_name=division_name,
                fetched_at=fetched_at,
                source_updated_at=source_updated_at,
            )
            division_matches = self._normalize_division_matches(
                event_connection,
                division_id=division_id,
                division_name=division_name,
                fetched_at=fetched_at,
                source_updated_at=source_updated_at,
            )
        finally:
            event_connection.close()

        if not division_rankings:
            raise RuntimeError("Local VEX Via cache did not contain any standings rows for the configured division")
        if not skills:
            warnings.append("Local VEX Via cache did not contain any skills rows for the configured division")
        if not division_matches:
            warnings.append("Local VEX Via cache did not contain any matches rows for the configured division")

        snapshot = self._extract_focal_snapshot(division_rankings, source_updated_at=source_updated_at)
        if snapshot is None:
            warnings.append(f"Focal team {self.settings.team_number} not found in local VEX Via standings cache")
        focal_matches = self._extract_focal_matches(division_matches)

        return RobotEventsResult(
            snapshot=snapshot,
            matches=focal_matches,
            division_rankings=division_rankings,
            skills=skills,
            division_matches=division_matches,
            snapshot_source="vex_via_local",
            warnings=warnings,
            result_tabs={
                "attempted_tabs": ["vex_via_local_rankings", "vex_via_local_skills", "vex_via_local_matches"],
                "successful_tabs": [
                    "vex_via_local_rankings",
                    *([] if not skills else ["vex_via_local_skills"]),
                    *([] if not division_matches else ["vex_via_local_matches"]),
                ],
                "dataset_tabs": {
                    "standings": ["vex_via_local_rankings"],
                    "skills": [] if not skills else ["vex_via_local_skills"],
                    "matches": [] if not focal_matches else ["vex_via_local_matches"],
                    "division_matches": [] if not division_matches else ["vex_via_local_matches"],
                },
                "request_urls": [],
            },
        )
