"""Unit tests for the VEX monitoring agent."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import httpx

import config
from collectors.media_web import MediaWebCollector
from collectors.robotevents import RobotEventsCollector
from collectors.vexvia_local import VexViaLocalCollector
import gui_app
import main
from notify.discord import confidence_allowed
from reporters.json_export import render_json_export
from reporters.markdown import render_markdown_report
from reporters.static_site import export_static_site, publish_to_git_repo
from storage import db
from utils.analysis import build_ai_rankings


class SettingsTests(unittest.TestCase):
    """Configuration tests."""

    def test_load_settings_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                self.assertEqual(settings.team_number, "7157B")
                self.assertFalse(settings.enable_optional_social)
                self.assertFalse(settings.enable_browser_fallback)
                self.assertTrue(settings.enable_vexvia_local)
                self.assertAlmostEqual(settings.power_rank_weight_official, 0.35)
                self.assertTrue(settings.data_dir.exists())


def _create_vexvia_fixture_db(path: Path) -> None:
    """Create a tiny VEX Via-style cache database."""
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE divisions (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE fields (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE teams (id INTEGER PRIMARY KEY, division_num INTEGER, number TEXT);
            CREATE TABLE rankings (
                id INTEGER PRIMARY KEY,
                division_id INTEGER,
                round INTEGER,
                team_id INTEGER,
                rank INTEGER,
                parm1 INTEGER,
                parm2 INTEGER,
                parm3 INTEGER,
                parm4 REAL,
                parm5 REAL,
                parm6 REAL,
                opr REAL,
                dpr REAL,
                ccwm REAL
            );
            CREATE TABLE matches (
                id INTEGER PRIMARY KEY,
                session TEXT,
                division_id INTEGER,
                round INTEGER,
                instance INTEGER,
                match INTEGER,
                time_scheduled TEXT,
                scored INTEGER,
                field_id INTEGER,
                red_score REAL,
                blue_score REAL,
                red_team1 TEXT,
                red_team2 TEXT,
                blue_team1 TEXT,
                blue_team2 TEXT
            );
            CREATE TABLE skills (
                id INTEGER PRIMARY KEY,
                team_id INTEGER,
                rank INTEGER,
                tie INTEGER,
                driver_attempts INTEGER,
                driver_score REAL,
                prog_attempts INTEGER,
                prog_score REAL,
                total_score REAL
            );
            CREATE TABLE last_modified (value TEXT);
            """
        )
        connection.execute("INSERT INTO divisions (id, name) VALUES (1, 'Technology')")
        connection.executemany(
            "INSERT INTO fields (id, name) VALUES (?, ?)",
            [(1, "Kettering"), (6, "Google")],
        )
        connection.executemany(
            "INSERT INTO teams (id, division_num, number) VALUES (?, ?, ?)",
            [(1, 1, "7157B"), (2, 1, "1234A"), (3, 1, "7777C"), (4, 1, "9999X"), (5, 1, "8888B"), (6, 1, "6666D")],
        )
        connection.executemany(
            """
            INSERT INTO rankings (
                id, division_id, round, team_id, rank, parm1, parm2, parm3, parm4, parm5, parm6, opr, dpr, ccwm
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 1, 200, 1, 31, 1, 0, 0, 2, 0, 32, 4.82, -0.44, 5.26),
                (2, 1, 200, 2, 1, 3, 0, 0, 6, 14, 40, 16.2, 6.1, 10.1),
            ],
        )
        connection.executemany(
            """
            INSERT INTO matches (
                id, session, division_id, round, instance, match, time_scheduled, scored, field_id,
                red_score, blue_score, red_team1, red_team2, blue_team1, blue_team2
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "Q", 1, 2, 1, 27, "2026-04-21T09:45:00-04:00", 0, 6, None, None, "7157B", "7777C", "1234A", "8888B"),
                (2, "Q", 1, 2, 1, 12, "2026-04-21T08:30:00-04:00", 1, 1, 22, 18, "7157B", "7777C", "1234A", "6666D"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO skills (
                id, team_id, rank, tie, driver_attempts, driver_score, prog_attempts, prog_score, total_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(1, 1, 10, 0, 3, 50, 2, 30, 80), (2, 2, 1, 0, 3, 60, 3, 35, 95)],
        )
        connection.execute("INSERT INTO last_modified (value) VALUES ('Tue, 21 Apr 2026 22:57:08 GMT')")
        connection.commit()
    finally:
        connection.close()


class StorageTests(unittest.TestCase):
    """SQLite behavior tests."""

    def test_schema_and_derived_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "monitor.db"
            with db.db_session(db_path) as connection:
                db.init_db(connection)
                snapshot = {
                    "event_sku": "RE",
                    "event_name": "Worlds",
                    "division_name": "Technology",
                    "team_number": "7157B",
                    "team_name": "Mystery Machine",
                    "school_name": "Chittenango",
                    "rank": 5,
                    "wins": 4,
                    "losses": 1,
                    "ties": 0,
                    "wp": 8,
                    "ap": 20,
                    "sp": 30,
                    "average_score": 42,
                    "record_text": "4-1-0",
                    "source": "api",
                    "fetched_at": "2026-04-21T12:00:00+00:00",
                }
                db.record_competition_snapshot(connection, snapshot)
                db.record_division_rankings(
                    connection,
                    "2026-04-21T12:00:00+00:00",
                    [
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "7157B",
                            "team_name": "Mystery Machine",
                            "organization": "Chittenango",
                            "rank": 2,
                            "wins": 5,
                            "losses": 1,
                            "ties": 0,
                            "wp": 10,
                            "ap": 20,
                            "sp": 25,
                            "average_score": 35,
                            "record_text": "5-1-0",
                        },
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "1234A",
                            "team_name": "Other",
                            "organization": "Org",
                            "rank": 1,
                            "wins": 6,
                            "losses": 0,
                            "ties": 0,
                            "wp": 12,
                            "ap": 22,
                            "sp": 26,
                            "average_score": 37,
                            "record_text": "6-0-0",
                        },
                    ],
                )
                db.record_skills_snapshot(
                    connection,
                    "2026-04-21T12:00:00+00:00",
                    [
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "7157B",
                            "team_name": "Mystery Machine",
                            "driver_score": 55,
                            "programming_score": 33,
                            "total_score": 88,
                            "source": "api",
                        },
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "1234A",
                            "team_name": "Other",
                            "driver_score": 60,
                            "programming_score": 35,
                            "total_score": 95,
                            "source": "api",
                        },
                    ],
                )
                db.upsert_division_matches(
                    connection,
                    [
                        {
                            "match_key": "Q1",
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "match_type": "qualification",
                            "round_label": "Q1",
                            "instance": 1,
                            "status": "completed",
                            "scheduled_time": "2026-04-21T12:30:00+00:00",
                            "completed_time": "2026-04-21T12:45:00+00:00",
                            "red_score": 20,
                            "blue_score": 18,
                            "red_teams": ["7157B", "5555C"],
                            "blue_teams": ["1234A", "9999X"],
                        },
                        {
                            "match_key": "Q2",
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "match_type": "qualification",
                            "round_label": "Q2",
                            "instance": 1,
                            "status": "completed",
                            "scheduled_time": "2026-04-21T13:00:00+00:00",
                            "completed_time": "2026-04-21T13:15:00+00:00",
                            "red_score": 16,
                            "blue_score": 22,
                            "red_teams": ["1234A", "9999X"],
                            "blue_teams": ["7157B", "5555C"],
                        },
                    ],
                )
                metrics = db.compute_and_store_derived_metrics(
                    connection,
                    snapshot_at="2026-04-21T12:00:00+00:00",
                    event_sku="RE",
                    division_name="Technology",
                    recent_match_count=5,
                    weights={
                        "official": 0.35,
                        "opr": 0.20,
                        "dpr": 0.10,
                        "ccwm": 0.15,
                        "skills": 0.10,
                        "form": 0.10,
                    },
                )
                snapshot["rank"] = 3
                snapshot["record_text"] = "5-1-0"
                snapshot["fetched_at"] = "2026-04-21T13:00:00+00:00"
                db.record_competition_snapshot(connection, snapshot)
                db.record_division_rankings(
                    connection,
                    "2026-04-21T13:00:00+00:00",
                    [
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "7157B",
                            "team_name": "Mystery Machine",
                            "organization": "Chittenango",
                            "rank": 3,
                            "wins": 5,
                            "losses": 1,
                            "ties": 0,
                            "wp": 10,
                            "ap": 20,
                            "sp": 25,
                            "average_score": 35,
                            "record_text": "5-1-0",
                            "source": "api",
                            "source_state": "live",
                            "result_tab": "api_rankings",
                            "source_updated_at": "2026-04-21T13:00:00+00:00",
                        },
                        {
                            "event_sku": "RE",
                            "division_name": "Technology",
                            "team_number": "1234A",
                            "team_name": "Other",
                            "organization": "Org",
                            "rank": 1,
                            "wins": 6,
                            "losses": 0,
                            "ties": 0,
                            "wp": 12,
                            "ap": 22,
                            "sp": 26,
                            "average_score": 37,
                            "record_text": "6-0-0",
                            "source": "api",
                            "source_state": "live",
                            "result_tab": "api_rankings",
                            "source_updated_at": "2026-04-21T13:00:00+00:00",
                        },
                    ],
                )
                db.compute_and_store_derived_metrics(
                    connection,
                    snapshot_at="2026-04-21T13:00:00+00:00",
                    event_sku="RE",
                    division_name="Technology",
                    recent_match_count=5,
                    weights={
                        "official": 0.35,
                        "opr": 0.20,
                        "dpr": 0.10,
                        "ccwm": 0.15,
                        "skills": 0.10,
                        "form": 0.10,
                    },
                )
                view = db.build_dashboard_view(connection, "7157B")
        self.assertTrue(metrics)
        self.assertEqual(view["latest_snapshot"]["team_number"], "7157B")
        self.assertTrue(view["team_power"]["power_rank"] >= 1)
        self.assertEqual(len(view["division_rankings"]), 2)
        self.assertEqual(view["rank_trend"]["sparkline"]["count"], 2)
        self.assertEqual(view["power_trend"]["sparkline"]["count"], 2)
        self.assertEqual(view["match_intelligence"]["last_match"]["match_key"], "Q2")
        self.assertIsNone(view["match_intelligence"]["next_match"])
        self.assertIn("1234A", view["match_intelligence"]["last_match"]["opponent_teams"])
        self.assertTrue(view["threat_list"])
        self.assertEqual(view["threat_list"][0]["team_number"], "1234A")
        self.assertTrue(view["alliance_impact"]["partner_rows"])
        self.assertEqual(view["alliance_impact"]["partner_rows"][0]["team_number"], "5555C")
        self.assertTrue(view["alliance_impact"]["opponent_rows"])
        self.assertIn("swing_matches", view)

    def test_ai_rankings_snapshot_overwrites_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "monitor.db"
            with db.db_session(db_path) as connection:
                db.init_db(connection)
                db.record_ai_rankings_snapshot(
                    connection,
                    "7157B",
                    {
                        "generated_at": "2026-04-21T12:00:00+00:00",
                        "source_snapshot_at": "2026-04-21T11:55:00+00:00",
                        "source_type": "vex_via_local",
                        "headline": "First headline",
                        "confidence": {"level": "high"},
                    },
                )
                db.record_ai_rankings_snapshot(
                    connection,
                    "7157B",
                    {
                        "generated_at": "2026-04-21T13:00:00+00:00",
                        "source_snapshot_at": "2026-04-21T12:55:00+00:00",
                        "source_type": "vex_via_local",
                        "headline": "Second headline",
                        "confidence": {"level": "moderate"},
                    },
                )
                stored = db.get_latest_ai_rankings(connection, "7157B")
        self.assertEqual(stored["headline"], "Second headline")
        self.assertEqual(stored["confidence"]["level"], "moderate")


class RobotEventsTests(unittest.TestCase):
    """RobotEvents collector tests."""

    def test_retry_then_success(self) -> None:
        calls = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["count"] += 1
            if calls["count"] == 1:
                raise httpx.ReadTimeout("timeout")
            if request.url.path.endswith("/events"):
                return httpx.Response(
                    200,
                    json={"data": [{"id": 1, "name": "Worlds", "divisions": [{"id": 2, "name": "Technology"}]}]},
                )
            if request.url.path.endswith("/rankings"):
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {
                                "rank": 2,
                                "wins": 6,
                                "losses": 1,
                                "ties": 0,
                                "wp": 12,
                                "ap": 44,
                                "sp": 53,
                                "average_score": 28,
                                "team": {"number": "7157B", "team_name": "Mystery Machine", "organization": "Chittenango"},
                            },
                            {
                                "rank": 1,
                                "wins": 7,
                                "losses": 0,
                                "ties": 0,
                                "wp": 14,
                                "ap": 45,
                                "sp": 60,
                                "average_score": 31,
                                "team": {"number": "1234A", "team_name": "Other", "organization": "Org"},
                            },
                        ]
                    },
                )
            if request.url.path.endswith("/skills"):
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {"team": {"number": "7157B", "team_name": "Mystery Machine"}, "driver_score": 50, "programming_score": 30, "total_score": 80}
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 10,
                            "name": "Q1",
                            "instance": 1,
                            "round": "qualification",
                            "scheduled": "2026-04-21T13:00:00+00:00",
                            "started": "2026-04-21T13:05:00+00:00",
                            "alliances": [
                                {"color": "red", "score": 20, "teams": [{"team": {"number": "7157B"}}, {"team": {"number": "5555C"}}]},
                                {"color": "blue", "score": 18, "teams": [{"team": {"number": "1234A"}}, {"team": {"number": "9999X"}}]},
                            ],
                        }
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport, headers={"Authorization": "token"})
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                settings.robotevents_api_key = "token"
                settings.http_backoff_base_seconds = 0
                collector = RobotEventsCollector(settings, client=client)
                result = collector.fetch()
        self.assertEqual(result.snapshot["rank"], 2)
        self.assertEqual(len(result.division_rankings), 2)
        self.assertEqual(len(result.skills), 1)
        self.assertEqual(len(result.matches), 1)

    def test_html_fallback_parser(self) -> None:
        fixture = (Path(__file__).parent / "fixtures" / "robotevents_sample.html").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                collector = RobotEventsCollector(
                    settings,
                    client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=""))),
                )
                snapshot, matches, division_rankings, skills_rows = collector.parse_rankings_html(fixture)
        self.assertEqual(snapshot["rank"], 7)
        self.assertEqual(snapshot["record_text"], "5-1-0")
        self.assertEqual(len(matches), 1)
        self.assertTrue(division_rankings)
        self.assertEqual(skills_rows, [])

    def test_missing_api_key_uses_public_fallback(self) -> None:
        fixture = (Path(__file__).parent / "fixtures" / "robotevents_sample.html").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                collector = RobotEventsCollector(
                    settings,
                    client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=fixture))),
                )
                result = collector.fetch()
        self.assertEqual(result.snapshot_source, "html_fallback")
        self.assertTrue(result.division_rankings)
        self.assertTrue(result.warnings)
        self.assertIn("event_page", result.result_tabs["attempted_tabs"])

    def test_browser_fallback_used_when_enabled_and_html_empty(self) -> None:
        empty_fixture = "<html><head><title>Worlds</title></head><body>No standings here.</body></html>"
        browser_rankings = [
            {
                "event_sku": "RE-V5RC-26-4025",
                "event_name": "Worlds",
                "division_name": "Technology",
                "team_number": "7157B",
                "team_name": "Mystery Machine",
                "organization": "Chittenango",
                "rank": 9,
                "wins": 4,
                "losses": 2,
                "ties": 0,
                "wp": 8,
                "ap": 15,
                "sp": 22,
                "average_score": 24,
                "record_text": "4-2-0",
                "source": "browser_fallback",
                "fetched_at": "2026-04-21T12:00:00+00:00",
            }
        ]
        browser_snapshot = {
            **browser_rankings[0],
            "school_name": "Chittenango",
        }
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                "os.environ",
                {"BASE_DIR": tmp, "ENABLE_BROWSER_FALLBACK": "true"},
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                collector = RobotEventsCollector(
                    settings,
                    client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=empty_fixture))),
                )
                with patch.object(
                    RobotEventsCollector,
                    "_browser_fallback_fetch",
                    return_value=(
                        browser_snapshot,
                        [],
                        browser_rankings,
                        [],
                        [],
                        {
                            "attempted_tabs": ["#results-", "rankings"],
                            "successful_tabs": ["rankings"],
                            "dataset_tabs": {"standings": ["rankings"], "skills": [], "matches": [], "division_matches": []},
                            "request_urls": [],
                        },
                    ),
                ):
                    result = collector.fetch()
        self.assertEqual(result.snapshot_source, "results_tab_browser")
        self.assertEqual(result.snapshot["rank"], 9)
        self.assertEqual(len(result.division_rankings), 1)
        self.assertTrue(any("trying browser fallback" in warning for warning in result.warnings))
        self.assertEqual(result.result_tabs["dataset_tabs"]["standings"], ["rankings"])

    def test_parse_division_list_text_extracts_roster(self) -> None:
        fixture = """
        Team List
        2026 VEX Robotics World Championship - Technology Division
        Team # Name School Location Age Group
        4610Z Zenith: Robot Rev Robot Revolution Summit, New Jersey, United States High School
        7157B Mystery Machine CHITTENANGO HIGH SCHOOL Chittenango, New York, United States High School
        99904W Wooosh NEW PALESTINE HIGH SCHOOL New Palestine, Indiana, United States High School
        Page 2 of 4 RE-V5RC-26-4025 April 8, 2026 6:23 PM
        """
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                collector = RobotEventsCollector(
                    settings,
                    client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=""))),
                )
                roster = collector._parse_division_list_text(fixture)
        self.assertEqual(len(roster), 3)
        team_numbers = [row["team_number"] for row in roster]
        self.assertIn("7157B", team_numbers)
        focal = next(row for row in roster if row["team_number"] == "7157B")
        self.assertEqual(focal["source"], "division_list_pdf")
        self.assertEqual(focal["record_text"], "Roster only")


class VexViaLocalTests(unittest.TestCase):
    """VEX Via local cache tests."""

    def test_local_cache_fetches_live_rankings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            event_db = tmp_path / "re-v5rc-26-4025.db"
            _create_vexvia_fixture_db(event_db)
            with patch.dict(
                "os.environ",
                {"BASE_DIR": tmp, "ENABLE_VEXVIA_LOCAL": "true", "VEXVIA_EVENT_DB_PATH": str(event_db)},
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                collector = VexViaLocalCollector(settings)
                result = collector.fetch()
        self.assertEqual(result.snapshot_source, "vex_via_local")
        self.assertEqual(result.snapshot["rank"], 31)
        self.assertEqual(result.snapshot["wp"], 2.0)
        self.assertEqual(result.snapshot["sp"], 32.0)
        self.assertEqual(result.snapshot["source_updated_at"], "2026-04-21T22:57:08+00:00")
        self.assertEqual(len(result.division_rankings), 2)
        self.assertEqual(len(result.skills), 2)
        self.assertEqual(len(result.division_matches), 2)
        self.assertTrue(any(match["match_key"] == "Q27" for match in result.matches))
        q27 = next(match for match in result.matches if match["match_key"] == "Q27")
        self.assertEqual(q27["field_name"], "Google")
        self.assertEqual(result.result_tabs["dataset_tabs"]["standings"], ["vex_via_local_rankings"])


class MediaCollectorTests(unittest.TestCase):
    """Media collector tests."""

    def test_media_dedupes_results_and_optional_social(self) -> None:
        rss = b"""
        <rss><channel>
          <item><title>Team 7157B update</title><link>https://example.com/a</link><pubDate>Tue, 21 Apr 2026 10:00:00 GMT</pubDate></item>
        </channel></rss>
        """
        html = """
        <html><body>
          <div class="result">
            <div class="result__title"><a href="https://example.com/a">Team 7157B update</a></div>
            <div class="result__snippet">snippet</div>
          </div>
          <div class="result">
            <div class="result__title"><a href="https://reddit.com/r/vex/comments/abc123">7157B reddit post</a></div>
            <div class="result__snippet">reddit snippet</div>
          </div>
        </body></html>
        """

        def handler(request: httpx.Request) -> httpx.Response:
            if "news.google.com" in request.url.host:
                return httpx.Response(200, content=rss)
            return httpx.Response(200, text=html)

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                "os.environ",
                {
                    "BASE_DIR": tmp,
                    "SEARCH_TERMS": "7157B",
                    "ENABLE_OPTIONAL_SOCIAL": "true",
                    "ENABLE_REDDIT": "true",
                },
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                collector = MediaWebCollector(settings, client=httpx.Client(transport=httpx.MockTransport(handler)))
                items = collector.fetch()
        self.assertGreaterEqual(len(items), 2)
        self.assertTrue(any(item["platform"] == "reddit" for item in items))

    def test_media_continues_when_one_source_fails(self) -> None:
        rss = b"""
        <rss><channel>
          <item><title>Team 7157B update</title><link>https://example.com/a</link><pubDate>Tue, 21 Apr 2026 10:00:00 GMT</pubDate></item>
        </channel></rss>
        """
        html = """
        <html><body>
          <div class="result">
            <div class="result__title"><a href="https://example.com/b">Team 7157B web hit</a></div>
            <div class="result__snippet">snippet</div>
          </div>
        </body></html>
        """

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "news.google.com" in request.url.host:
                return httpx.Response(200, content=rss)
            if "site%3Ayoutube.com" in url:
                return httpx.Response(403, text="forbidden")
            return httpx.Response(200, text=html)

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                "os.environ",
                {
                    "BASE_DIR": tmp,
                    "SEARCH_TERMS": "7157B",
                    "ENABLE_YOUTUBE": "true",
                },
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                collector = MediaWebCollector(settings, client=httpx.Client(transport=httpx.MockTransport(handler)))
                items = collector.fetch()

        self.assertTrue(any(item["url"] == "https://example.com/a" for item in items))
        self.assertTrue(any(item["url"] == "https://example.com/b" for item in items))


class ReporterTests(unittest.TestCase):
    """Report rendering tests."""

    def test_report_renderers(self) -> None:
        view = {
            "latest_snapshot": {
                "team_number": "7157B",
                "division_name": "Technology",
                "rank": 4,
                "record_text": "4-1-0",
                "event_name": "Worlds",
                "team_name": "Mystery Machine",
                "school_name": "Chittenango",
                "wp": 8,
                "ap": 20,
                "sp": 30,
                "average_score": 42,
                "fetched_at": "2026-04-21T12:00:00+00:00",
            },
            "previous_snapshot": None,
            "delta": {"rank_change": None, "rank_direction": "no change", "record_changed": False},
            "team_power": {"power_rank": 3, "opr": 20.0, "dpr": 18.0, "ccwm": 2.0, "recent_form": 1.5, "composite_score": 0.88},
            "power_delta": {"power_rank_change": None, "power_rank_direction": "no change"},
            "team_skill": {"total_score": 88, "driver_score": 55, "programming_score": 33},
            "recent_completed_matches": [],
            "upcoming_matches": [],
            "recent_media": [],
            "collector_runs": [],
            "snapshot_history": [],
            "analysis": {"headline": "Team 7157B remains competitive.", "cards": []},
            "ai_rankings": {
                "generated_at": "2026-04-21T12:05:00+00:00",
                "source_snapshot_at": "2026-04-21T12:00:00+00:00",
                "source_type": "vex_via_local",
                "headline": "7157B sits at official rank #4 and power rank #3 with high confidence.",
                "why_it_matters": "Nearby teams can compress the standings quickly.",
                "priority_factors": ["Top nearby threat is 1234A."],
                "confidence": {"level": "high", "body": "Ranking confidence is high."},
            },
            "rankings_status": {
                "rankings_count": 1,
                "skills_count": 0,
                "power_count": 1,
                "snapshot_source": "api",
                "source_state": "live",
                "result_tabs": {"standings": ["api_rankings"], "skills": [], "division_matches": []},
            },
            "division_rankings": [{"rank": 1, "team_number": "1234A", "record_text": "6-0-0", "wp": 12, "ap": 20, "sp": 30}],
            "skills_rankings": [],
            "power_rankings": [{"power_rank": 1, "team_number": "1234A", "composite_score": 0.99, "opr": 22, "dpr": 17, "ccwm": 5}],
            "biggest_movers": [],
        }
        markdown = render_markdown_report(view)
        payload = render_json_export(view)
        self.assertIn("Team 7157B is currently ranked #4", markdown)
        self.assertIn("Automated analysis: Team 7157B remains competitive.", markdown)
        self.assertIn("## AI Rankings", markdown)
        self.assertIn("AI rankings: 7157B sits at official rank #4", markdown)
        self.assertIn("Result-tab coverage: standings api_rankings", markdown)
        self.assertIn("Top Power Rankings", markdown)
        self.assertEqual(payload["team_power"]["power_rank"], 3)
        self.assertEqual(payload["analysis"]["headline"], "Team 7157B remains competitive.")
        self.assertEqual(payload["ai_rankings"]["source_type"], "vex_via_local")


class DiscordTests(unittest.TestCase):
    """Notification helper tests."""

    def test_confidence_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp, "MEDIA_CONFIDENCE_NOTIFY_MIN": "trusted"}, clear=True):
                settings = config.load_settings(env_file=None)
        self.assertFalse(confidence_allowed(settings, {"confidence": "unverified"}))
        self.assertTrue(confidence_allowed(settings, {"confidence": "official"}))


class GuiTests(unittest.TestCase):
    """GUI rendering tests."""

    def test_routes_render_with_empty_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                with db.db_session(settings.db_path) as connection:
                    db.init_db(connection)
                app = gui_app.create_app()
                captured: dict[str, object] = {}

                def start_response(status, headers):
                    captured["status"] = status
                    captured["headers"] = headers

                dashboard_body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/", "QUERY_STRING": ""}, start_response))
                rankings_body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/rankings", "QUERY_STRING": ""}, start_response))
                analysis_body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/analysis", "QUERY_STRING": ""}, start_response))
                ai_rankings_body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/ai-rankings", "QUERY_STRING": ""}, start_response))
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn(b"Team 7157B Monitoring Console", dashboard_body)
        self.assertIn(b"No rankings data has been collected yet.", rankings_body)
        self.assertIn(b"No reliable official standings source is active yet.", analysis_body)
        self.assertIn(b"No AI rankings snapshot is available yet.", ai_rankings_body)

    def test_rankings_refresh_action_redirects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                with db.db_session(settings.db_path) as connection:
                    db.init_db(connection)
                app = gui_app.create_app()
                captured: dict[str, object] = {}

                def start_response(status, headers):
                    captured["status"] = status
                    captured["headers"] = headers

                with patch("gui_app.run_competition_cycle", return_value={"division_rankings": [{"team_number": "7157B"}]}):
                    _ = b"".join(app({"REQUEST_METHOD": "POST", "PATH_INFO": "/actions/refresh-rankings", "QUERY_STRING": ""}, start_response))
        self.assertEqual(captured["status"], "302 Found")
        self.assertTrue(any(header[0] == "Location" and "Rankings+refresh+completed:+1+teams" in header[1] for header in captured["headers"]))

    def test_ai_rankings_refresh_action_redirects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                with db.db_session(settings.db_path) as connection:
                    db.init_db(connection)
                app = gui_app.create_app()
                captured: dict[str, object] = {}

                def start_response(status, headers):
                    captured["status"] = status
                    captured["headers"] = headers

                with patch(
                    "gui_app.run_ai_rankings_cycle",
                    return_value={"confidence": {"level": "high"}},
                ):
                    _ = b"".join(app({"REQUEST_METHOD": "POST", "PATH_INFO": "/actions/refresh-ai-rankings", "QUERY_STRING": ""}, start_response))
        self.assertEqual(captured["status"], "302 Found")
        self.assertTrue(any(header[0] == "Location" and "AI+rankings+refresh+completed:+high" in header[1] for header in captured["headers"]))


class AIRankingsTests(unittest.TestCase):
    """AI rankings synthesis tests."""

    def test_build_ai_rankings_handles_partial_view(self) -> None:
        payload = build_ai_rankings(
            {
                "latest_snapshot": None,
                "team_power": None,
                "team_skill": None,
                "rankings_status": {"snapshot_source": "division_list_pdf", "source_state": "roster_only"},
                "threat_list": [],
                "swing_matches": [],
                "alliance_impact": {"partner_rows": [], "opponent_rows": []},
                "biggest_movers": [],
                "rank_trend": {},
                "power_trend": {},
                "match_intelligence": {},
                "delta": {},
                "power_delta": {},
                "recent_completed_matches": [],
                "upcoming_matches": [],
                "recent_media": [],
                "collector_runs": [],
                "division_rankings": [],
                "skills_rankings": [],
                "power_rankings": [],
            }
        )
        self.assertIn("does not have enough current standings data", payload["headline"])
        self.assertEqual(payload["confidence"]["level"], "low")


class StaticExportTests(unittest.TestCase):
    """Static site export and publish-guard tests."""

    def test_static_export_writes_pages_and_json_without_live_controls(self) -> None:
        repo_base = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "monitor.db"
            site_dir = Path(tmp) / "site"
            with patch.dict(
                "os.environ",
                {
                    "BASE_DIR": str(repo_base),
                    "DB_PATH": str(db_path),
                    "STATIC_SITE_DIR": str(site_dir),
                },
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                with db.db_session(settings.db_path) as connection:
                    db.init_db(connection)
                    snapshot = {
                        "event_sku": "RE",
                        "event_name": "Worlds",
                        "division_name": "Technology",
                        "team_number": "7157B",
                        "team_name": "Mystery Machine",
                        "school_name": "Chittenango",
                        "rank": 4,
                        "wins": 2,
                        "losses": 1,
                        "ties": 0,
                        "wp": 4,
                        "ap": 6,
                        "sp": 22,
                        "average_score": 30,
                        "record_text": "2-1-0",
                        "source": "vex_via_local",
                        "fetched_at": "2026-04-22T12:00:00+00:00",
                    }
                    db.record_competition_snapshot(connection, snapshot)
                    db.record_division_rankings(
                        connection,
                        "2026-04-22T12:00:00+00:00",
                        [
                            {
                                "event_sku": "RE",
                                "division_name": "Technology",
                                "team_number": "7157B",
                                "team_name": "Mystery Machine",
                                "organization": "Chittenango",
                                "rank": 4,
                                "wins": 2,
                                "losses": 1,
                                "ties": 0,
                                "wp": 4,
                                "ap": 6,
                                "sp": 22,
                                "average_score": 30,
                                "record_text": "2-1-0",
                                "source": "vex_via_local",
                                "source_state": "live",
                                "result_tab": "vex_via_local",
                                "source_updated_at": "2026-04-22T12:00:00+00:00",
                            }
                        ],
                    )
                    db.record_skills_snapshot(
                        connection,
                        "2026-04-22T12:00:00+00:00",
                        [
                            {
                                "event_sku": "RE",
                                "division_name": "Technology",
                                "team_number": "7157B",
                                "team_name": "Mystery Machine",
                                "driver_score": 40,
                                "programming_score": 20,
                                "total_score": 60,
                                "source": "vex_via_local",
                            }
                        ],
                    )
                    db.record_ai_rankings_snapshot(
                        connection,
                        "7157B",
                        {
                            "generated_at": "2026-04-22T12:05:00+00:00",
                            "source_snapshot_at": "2026-04-22T12:00:00+00:00",
                            "source_type": "vex_via_local",
                            "confidence": {"level": "high", "body": "Fresh local standings available."},
                            "headline": "7157B sits at official rank #4 with high confidence.",
                            "why_it_matters": "The team is in the upper part of the division.",
                            "official_rank": 4,
                            "power_rank": None,
                            "skills_total": 60,
                            "summary_blocks": [],
                            "priority_factors": ["Protect the top-10 trajectory."],
                            "threat_rows": [],
                            "swing_rows": [],
                            "alliance": {},
                            "top_movers": [],
                            "trend": {},
                        },
                    )
                    view = db.build_dashboard_view(connection, settings.team_number)
                result = export_static_site(repo_base, settings, view)
                self.assertTrue((site_dir / "index.html").exists())
                self.assertTrue((site_dir / "rankings" / "index.html").exists())
                self.assertTrue((site_dir / "data" / "latest.json").exists())
                dashboard_html = (site_dir / "index.html").read_text(encoding="utf-8")
                settings_html = (site_dir / "settings" / "index.html").read_text(encoding="utf-8")
                self.assertIn("Snapshot Dashboard", dashboard_html)
                self.assertNotIn("Run Refresh", dashboard_html)
                self.assertNotIn(str(db_path), settings_html)
                self.assertEqual(Path(result["site_dir"]).resolve(), site_dir.resolve())

    def test_publish_to_git_repo_requires_configured_repo(self) -> None:
        repo_base = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                "os.environ",
                {
                    "BASE_DIR": str(repo_base),
                    "STATIC_SITE_DIR": str(Path(tmp) / "site"),
                },
                clear=True,
            ):
                settings = config.load_settings(env_file=None)
                settings.static_site_dir.mkdir(parents=True, exist_ok=True)
                result = publish_to_git_repo(settings)
        self.assertFalse(result["published"])
        self.assertIn("GITHUB_PAGES_REPO", result["reason"])


class MainTests(unittest.TestCase):
    """Scheduler tests."""

    def test_scheduler_registers_ai_rankings_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"BASE_DIR": tmp}, clear=True):
                settings = config.load_settings(env_file=None)
                scheduler = main.build_scheduler(settings)
        job_ids = {job.id for job in scheduler.get_jobs()}
        self.assertIn("ai_rankings", job_ids)

    def test_parse_args_accepts_publish_static(self) -> None:
        with patch("sys.argv", ["main.py", "--publish-static"]):
            args = main.parse_args()
        self.assertTrue(args.publish_static)

if __name__ == "__main__":
    unittest.main()
