"""Official RobotEvents collector with a lightweight HTML fallback."""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import logging
import re
import time
from typing import Any
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup
import httpx

from config import Settings
from storage.db import utc_now

LOGGER = logging.getLogger(__name__)


class RateLimiter:
    """Simple minimum-interval rate limiter."""

    def __init__(self, per_minute: int) -> None:
        self.minimum_interval = 60.0 / max(per_minute, 1)
        self._last_request = 0.0

    def wait(self) -> None:
        """Sleep long enough to respect the minimum request interval."""
        now = time.monotonic()
        delay = self.minimum_interval - (now - self._last_request)
        if delay > 0:
            time.sleep(delay)
        self._last_request = time.monotonic()


@dataclass(slots=True)
class RobotEventsResult:
    """Normalized official competition payload."""

    snapshot: dict[str, Any] | None
    matches: list[dict[str, Any]]
    division_rankings: list[dict[str, Any]]
    skills: list[dict[str, Any]]
    division_matches: list[dict[str, Any]]
    snapshot_source: str
    warnings: list[str]
    result_tabs: dict[str, Any]


class RobotEventsCollector:
    """Collect official event and team status from RobotEvents."""

    def __init__(self, settings: Settings, client: httpx.Client | None = None) -> None:
        self.settings = settings
        self.client = client or httpx.Client(
            timeout=settings.request_timeout_seconds,
            headers={
                "Accept": "application/json",
                "User-Agent": "vex-ranker-monitor/1.0",
                "Authorization": settings.robotevents_api_key,
            },
        )
        self._managed_client = client is None
        self._limiter = RateLimiter(settings.http_rate_limit_per_minute)

    def close(self) -> None:
        """Close the owned HTTP client."""
        if self._managed_client:
            self.client.close()

    def _request(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        accept: str = "application/json",
    ) -> httpx.Response:
        """Perform an HTTP request with retry and backoff."""
        headers = dict(self.client.headers)
        headers["Accept"] = accept
        last_error: Exception | None = None
        for attempt in range(1, self.settings.http_max_retries + 1):
            self._limiter.wait()
            try:
                response = self.client.get(url, params=params, headers=headers)
                if response.status_code >= 500:
                    response.raise_for_status()
                return response
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_error = exc
                LOGGER.warning(
                    "RobotEvents request failed",
                    extra={"collector": "robotevents", "error": str(exc)},
                )
                if attempt >= self.settings.http_max_retries:
                    break
                time.sleep(self.settings.http_backoff_base_seconds ** attempt)
        raise RuntimeError(f"RobotEvents request failed after retries: {last_error}") from last_error

    def _api_get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetch JSON from the API."""
        response = self._request(f"{self.settings.robotevents_api_base}{path}", params=params)
        response.raise_for_status()
        return response.json()

    def _safe_api_get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        warning_label: str,
        warnings: list[str],
    ) -> dict[str, Any] | None:
        """Fetch JSON from the API, but degrade to a warning instead of exploding."""
        try:
            return self._api_get(path, params=params)
        except Exception as exc:
            warnings.append(f"{warning_label}: {exc}")
            LOGGER.warning(
                "RobotEvents API sub-request failed",
                extra={"collector": "robotevents", "error": str(exc)},
            )
            return None

    def _fetch_event_payload(self) -> dict[str, Any]:
        """Look up the event record by SKU."""
        payload = self._api_get("/events", params={"sku[]": self.settings.event_sku})
        data = payload.get("data") or []
        if not data:
            raise RuntimeError(f"Event not found for SKU {self.settings.event_sku}")
        return data[0]

    def _find_division(self, event_payload: dict[str, Any]) -> dict[str, Any]:
        """Find the configured division in the event payload."""
        divisions = event_payload.get("divisions") or []
        for division in divisions:
            if str(division.get("name", "")).strip().lower() == self.settings.division_name.lower():
                return division
        if divisions:
            return divisions[0]
        raise RuntimeError("Division metadata missing from event payload")

    def _fetch_rankings_payload(self, event_id: int, division_id: int) -> dict[str, Any]:
        """Fetch the division rankings.

        Endpoint shapes can differ across RobotEvents API releases.
        """
        return self._api_get(f"/events/{event_id}/divisions/{division_id}/rankings")

    def _fetch_matches_payload(self, event_id: int, division_id: int) -> dict[str, Any]:
        """Fetch division matches for the configured event."""
        return self._api_get(f"/events/{event_id}/divisions/{division_id}/matches")

    def _fetch_skills_payload(self, event_id: int, division_id: int) -> dict[str, Any]:
        """Fetch skills data for the event or division.

        Depending on the API release, this path may need adjustment to the current
        RobotEvents v2 skills endpoint shape.
        """
        return self._api_get(f"/events/{event_id}/divisions/{division_id}/skills")

    def _normalize_division_rankings(
        self,
        event_payload: dict[str, Any],
        division_payload: dict[str, Any],
        rankings_payload: dict[str, Any],
        fetched_at: str,
    ) -> list[dict[str, Any]]:
        """Normalize the full division rankings payload."""
        rankings: list[dict[str, Any]] = []
        for item in rankings_payload.get("data") or []:
            team = item.get("team") or {}
            wins = item.get("wins")
            losses = item.get("losses")
            ties = item.get("ties")
            rankings.append(
                {
                    "event_sku": self.settings.event_sku,
                    "event_name": event_payload.get("name", self.settings.event_name_alias),
                    "division_name": division_payload.get("name", self.settings.division_name),
                    "team_number": team.get("number") or team.get("name") or "",
                    "team_name": team.get("team_name") or team.get("name") or "",
                    "organization": team.get("organization") or "",
                    "rank": item.get("rank"),
                    "wins": wins,
                    "losses": losses,
                    "ties": ties,
                    "wp": item.get("wp"),
                    "ap": item.get("ap"),
                    "sp": item.get("sp"),
                    "average_score": item.get("average_score"),
                    "record_text": f"{wins}-{losses}-{ties}",
                    "source": "api",
                    "source_state": "live",
                    "result_tab": "api_rankings",
                    "fetched_at": fetched_at,
                }
            )
        return rankings

    def _extract_focal_snapshot(self, rankings: list[dict[str, Any]]) -> dict[str, Any]:
        """Extract the focal team snapshot from division rankings."""
        for item in rankings:
            if str(item.get("team_number", "")).lower() == self.settings.team_number.lower():
                return {
                    **item,
                    "school_name": item.get("organization") or self.settings.school_alias,
                }
        raise RuntimeError(f"Team {self.settings.team_number} not found in division rankings")

    def _extract_team_numbers(self, alliance: dict[str, Any]) -> list[str]:
        """Return team numbers from an alliance payload."""
        team_numbers: list[str] = []
        for team_row in alliance.get("teams") or []:
            team = team_row.get("team") or {}
            number = team.get("number") or team.get("name")
            if number:
                team_numbers.append(str(number))
        return team_numbers

    def _normalize_division_matches(
        self,
        matches_payload: dict[str, Any],
        division_name: str,
    ) -> list[dict[str, Any]]:
        """Normalize division-wide match rows."""
        matches: list[dict[str, Any]] = []
        for item in matches_payload.get("data") or []:
            alliances = item.get("alliances") or []
            red = alliances[0] if len(alliances) > 0 else {}
            blue = alliances[1] if len(alliances) > 1 else {}
            red_teams = self._extract_team_numbers(red)
            blue_teams = self._extract_team_numbers(blue)
            red_score = red.get("score")
            blue_score = blue.get("score")
            status = "completed" if red_score is not None and blue_score is not None else "scheduled"
            matches.append(
                {
                    "match_key": str(item.get("id") or item.get("name")),
                    "event_sku": self.settings.event_sku,
                    "division_name": division_name,
                    "match_type": item.get("round"),
                    "round_label": item.get("name") or str(item.get("id")),
                    "instance": item.get("instance"),
                    "status": status,
                    "scheduled_time": item.get("scheduled"),
                    "completed_time": item.get("started"),
                    "red_score": red_score,
                    "blue_score": blue_score,
                    "red_teams": red_teams,
                    "blue_teams": blue_teams,
                }
            )
        return matches

    def _extract_focal_matches(self, division_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract the focal team matches from division-wide matches."""
        results: list[dict[str, Any]] = []
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
            results.append(
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
                    "alliance": alliance,
                    "opponent": ", ".join(opponents) if opponents else "TBD",
                    "score_for": score_for,
                    "score_against": score_against,
                }
            )
        return results

    def _normalize_skills(
        self,
        skills_payload: dict[str, Any],
        division_name: str,
        fetched_at: str,
    ) -> list[dict[str, Any]]:
        """Normalize skills rows."""
        rows: list[dict[str, Any]] = []
        for item in skills_payload.get("data") or []:
            team = item.get("team") or {}
            driver = item.get("driver") or item.get("driver_score") or 0
            programming = item.get("programming") or item.get("programming_score") or 0
            total = item.get("score") or item.get("total_score") or (float(driver) + float(programming))
            rows.append(
                {
                    "event_sku": self.settings.event_sku,
                    "division_name": division_name,
                    "team_number": team.get("number") or team.get("name") or "",
                    "team_name": team.get("team_name") or team.get("name") or "",
                    "driver_score": driver,
                    "programming_score": programming,
                    "total_score": total,
                    "source": "api",
                    "source_state": "live",
                    "result_tab": "api_skills",
                    "fetched_at": fetched_at,
                }
            )
        return rows

    def _source_state(self, source: str, *, has_rankings: bool) -> str:
        """Return a normalized source-state label."""
        if source == "api":
            return "live"
        if source in {"results_tab_browser", "html_fallback"} and has_rankings:
            return "live"
        if source == "division_list_pdf":
            return "roster_only"
        return "partial"

    def _fallback_url(self) -> str:
        """Build a fallback RobotEvents URL for the event."""
        return f"https://www.robotevents.com/robot-competitions/vex-robotics-competition/{quote_plus(self.settings.event_sku)}.html"

    def _results_anchor_url(self) -> str:
        """Return the event results anchor URL."""
        return f"{self._fallback_url()}#results-"

    def _division_list_page_url(self) -> str:
        """Return the public division list index page."""
        return "https://recf.org/vex_worlds/division-lists/"

    def _find_division_list_pdf_url(self, html_text: str) -> str | None:
        """Find the division-list PDF URL for the configured division."""
        soup = BeautifulSoup(html_text, "html.parser")
        target = self.settings.division_name.strip().lower()
        for anchor in soup.find_all("a", href=True):
            text = anchor.get_text(" ", strip=True).lower()
            href = str(anchor.get("href") or "").strip()
            normalized_href = href.lower().rstrip("/")
            if target in text and ".pdf" in normalized_href:
                return urljoin(self._division_list_page_url(), href)
        return None

    def _parse_division_list_text(self, extracted_text: str) -> list[dict[str, Any]]:
        """Parse extracted PDF text into a team roster.

        This is a roster fallback, not live standings. Rank/record fields stay empty.
        """
        roster: list[dict[str, Any]] = []
        cleaned_lines: list[str] = []
        for raw_line in extracted_text.replace("\r", "\n").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("Page "):
                continue
            if line in {
                "Team List",
                f"2026 VEX Robotics World Championship - {self.settings.division_name} Division",
                "Team # Name School Location Age Group",
            }:
                continue
            cleaned_lines.append(line)

        normalized = re.sub(r"\s+", " ", " ".join(cleaned_lines)).strip()
        entry_pattern = re.compile(
            r"(?:(?<=\s)|^)(?P<number>\d{1,5}[A-Z])\s+"
            r"(?P<body>.*?)(?=(?:(?<=\s)\d{1,5}[A-Z]\s)|$)"
        )
        entries = [
            f"{match.group('number')} {match.group('body').strip()}".strip()
            for match in entry_pattern.finditer(normalized)
        ]

        school_markers = (
            " HIGH SCHOOL",
            " High School",
            " SCHOOL",
            " School",
            " Academy",
            " ACADEMY",
            " College",
            " COLLEGE",
            " Robotics",
            " ROBOTICS",
        )

        for entry in entries:
            match = re.match(r"^(?P<number>\d{1,5}[A-Z])\s+(?P<body>.+)$", entry)
            if not match:
                continue
            team_number = match.group("number")
            body = re.sub(r"\s+", " ", match.group("body")).strip(" ,")
            first_marker = min(
                (body.find(marker) for marker in school_markers if marker in body),
                default=-1,
            )
            if first_marker > 0:
                team_name = body[:first_marker].strip(" ,")
                organization = body[first_marker:].strip(" ,")
            else:
                team_name = body
                organization = ""
            roster.append(
                {
                    "event_sku": self.settings.event_sku,
                    "event_name": self.settings.event_name_alias,
                    "division_name": self.settings.division_name,
                    "team_number": team_number,
                    "team_name": team_name,
                    "organization": organization,
                    "rank": None,
                    "wins": None,
                    "losses": None,
                    "ties": None,
                    "wp": None,
                    "ap": None,
                    "sp": None,
                    "average_score": None,
                    "record_text": "Roster only",
                    "source": "division_list_pdf",
                    "source_state": "roster_only",
                    "result_tab": "division_list_pdf",
                    "fetched_at": utc_now(),
                }
            )
        return roster

    def _fetch_division_list_roster(self, warnings: list[str]) -> list[dict[str, Any]]:
        """Fetch the public division-list PDF and extract the team roster."""
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            warnings.append("pypdf is not installed, so PDF roster fallback is unavailable")
            raise RuntimeError("PDF roster fallback unavailable") from exc

        index_response = self._request(self._division_list_page_url(), accept="text/html")
        index_response.raise_for_status()
        pdf_url = self._find_division_list_pdf_url(index_response.text)
        if not pdf_url:
            raise RuntimeError(f"Division list PDF not found for {self.settings.division_name}")
        pdf_response = self._request(pdf_url, accept="application/pdf")
        pdf_response.raise_for_status()
        reader = PdfReader(BytesIO(pdf_response.content))
        extracted_text = "\n".join(page.extract_text() or "" for page in reader.pages)
        roster = self._parse_division_list_text(extracted_text)
        if not roster:
            raise RuntimeError("Division list PDF parsed, but no team roster was extracted")
        return roster

    def parse_results_html(self, html_text: str, *, source: str = "html_fallback", result_tab: str = "event_page") -> dict[str, Any]:
        """Parse standings, skills, and matches from results-oriented HTML."""
        soup = BeautifulSoup(html_text, "html.parser")
        fetched_at = utc_now()
        title = soup.title.get_text(strip=True) if soup.title else self.settings.event_name_alias
        focal_snapshot = {
            "event_sku": self.settings.event_sku,
            "event_name": title,
            "division_name": self.settings.division_name,
            "team_number": self.settings.team_number,
            "team_name": self.settings.team_name_alias,
            "school_name": self.settings.school_alias,
            "rank": None,
            "wins": None,
            "losses": None,
            "ties": None,
            "wp": None,
            "ap": None,
            "sp": None,
            "average_score": None,
            "record_text": "Unknown",
            "source": source,
            "source_state": "partial",
            "result_tab": result_tab,
            "fetched_at": fetched_at,
        }
        division_rankings: list[dict[str, Any]] = []
        skills_rows: list[dict[str, Any]] = []
        focal_matches: list[dict[str, Any]] = []
        division_matches: list[dict[str, Any]] = []
        tab_hits: set[str] = set()

        for table in soup.select("table"):
            headers = [th.get_text(" ", strip=True).lower() for th in table.select("thead th")]
            if "rank" in headers and "team" in headers:
                tab_hits.add("rankings")
                for row in table.select("tbody tr"):
                    cells = [td.get_text(" ", strip=True) for td in row.select("td")]
                    if len(cells) < len(headers):
                        continue
                    team_text = cells[headers.index("team")]
                    team_number = team_text.split()[0]
                    item = {
                        "event_sku": self.settings.event_sku,
                        "event_name": title,
                        "division_name": self.settings.division_name,
                        "team_number": team_number,
                        "team_name": " ".join(team_text.split()[1:]),
                        "organization": "",
                        "rank": int(cells[headers.index("rank")]) if cells[headers.index("rank")].isdigit() else None,
                        "wins": int(cells[headers.index("wins")]) if "wins" in headers and cells[headers.index("wins")].isdigit() else None,
                        "losses": int(cells[headers.index("losses")]) if "losses" in headers and cells[headers.index("losses")].isdigit() else None,
                        "ties": int(cells[headers.index("ties")]) if "ties" in headers and cells[headers.index("ties")].isdigit() else None,
                        "wp": cells[headers.index("wp")] if "wp" in headers else None,
                        "ap": cells[headers.index("ap")] if "ap" in headers else None,
                        "sp": cells[headers.index("sp")] if "sp" in headers else None,
                        "average_score": None,
                        "record_text": "-".join([cells[headers.index(name)] for name in ("wins", "losses", "ties") if name in headers]),
                        "source": source,
                        "source_state": self._source_state(source, has_rankings=True),
                        "result_tab": result_tab,
                        "fetched_at": fetched_at,
                    }
                    division_rankings.append(item)
                    if self.settings.team_number == team_number:
                        focal_snapshot = {**item, "school_name": self.settings.school_alias}

            if "team" in headers and any("driver" in header for header in headers) and any("program" in header for header in headers):
                tab_hits.add("skills")
                for row in table.select("tbody tr"):
                    cells = [td.get_text(" ", strip=True) for td in row.select("td")]
                    if len(cells) < len(headers):
                        continue
                    team_text = cells[headers.index("team")]
                    team_number = team_text.split()[0]
                    driver_value = cells[headers.index(next(header for header in headers if "driver" in header))]
                    programming_value = cells[headers.index(next(header for header in headers if "program" in header))]
                    total_value = None
                    for total_header in ("total", "highscore", "score"):
                        if total_header in headers:
                            total_value = cells[headers.index(total_header)]
                            break
                    try:
                        driver_score = float(driver_value)
                    except ValueError:
                        driver_score = 0.0
                    try:
                        programming_score = float(programming_value)
                    except ValueError:
                        programming_score = 0.0
                    try:
                        total_score = float(total_value) if total_value is not None else driver_score + programming_score
                    except ValueError:
                        total_score = driver_score + programming_score
                    skills_rows.append(
                        {
                            "event_sku": self.settings.event_sku,
                            "division_name": self.settings.division_name,
                            "team_number": team_number,
                            "team_name": " ".join(team_text.split()[1:]),
                            "driver_score": driver_score,
                            "programming_score": programming_score,
                            "total_score": total_score,
                            "source": source,
                            "source_state": self._source_state(source, has_rankings=bool(division_rankings)),
                            "result_tab": result_tab,
                            "fetched_at": fetched_at,
                        }
                    )

            if any("match" in header for header in headers):
                has_red = any("red" in header for header in headers)
                has_blue = any("blue" in header for header in headers)
                if has_red and has_blue:
                    tab_hits.add("matches")
                    for row in table.select("tbody tr"):
                        cells = [td.get_text(" ", strip=True) for td in row.select("td")]
                        if len(cells) < len(headers):
                            continue
                        row_map = {headers[index]: cells[index] for index in range(len(headers))}
                        match_key = row_map.get("match") or row_map.get("matches") or cells[0]
                        red_teams = [value for key, value in row_map.items() if "red" in key and "score" not in key and value]
                        blue_teams = [value for key, value in row_map.items() if "blue" in key and "score" not in key and value]
                        red_score_raw = next((row_map[key] for key in row_map if "red" in key and "score" in key), "")
                        blue_score_raw = next((row_map[key] for key in row_map if "blue" in key and "score" in key), "")
                        red_score = float(red_score_raw) if red_score_raw.replace(".", "", 1).isdigit() else None
                        blue_score = float(blue_score_raw) if blue_score_raw.replace(".", "", 1).isdigit() else None
                        division_match = {
                            "match_key": match_key,
                            "event_sku": self.settings.event_sku,
                            "division_name": self.settings.division_name,
                            "match_type": "unknown",
                            "round_label": match_key,
                            "instance": None,
                            "status": "completed" if red_score is not None and blue_score is not None else "scheduled",
                            "scheduled_time": None,
                            "completed_time": None,
                            "red_score": red_score,
                            "blue_score": blue_score,
                            "red_teams": red_teams,
                            "blue_teams": blue_teams,
                            "source": source,
                            "source_state": self._source_state(source, has_rankings=bool(division_rankings)),
                            "result_tab": result_tab,
                        }
                        division_matches.append(division_match)
                        joined = " ".join(cells)
                        if self.settings.team_number in joined:
                            focal_matches.append(
                                {
                                    "match_key": match_key,
                                    "event_sku": self.settings.event_sku,
                                    "division_name": self.settings.division_name,
                                    "team_number": self.settings.team_number,
                                    "match_type": "unknown",
                                    "round_label": match_key,
                                    "instance": None,
                                    "status": division_match["status"],
                                    "scheduled_time": None,
                                    "completed_time": None,
                                    "alliance": None,
                                    "opponent": joined,
                                    "score_for": None,
                                    "score_against": None,
                                }
                            )
                elif "team" in headers:
                    tab_hits.add("matches")
                    for row in table.select("tbody tr"):
                        cells = [td.get_text(" ", strip=True) for td in row.select("td")]
                        joined = " ".join(cells)
                        if self.settings.team_number not in joined:
                            continue
                        match_key = cells[0]
                        focal_matches.append(
                            {
                                "match_key": match_key,
                                "event_sku": self.settings.event_sku,
                                "division_name": self.settings.division_name,
                                "team_number": self.settings.team_number,
                                "match_type": "unknown",
                                "round_label": match_key,
                                "instance": None,
                                "status": "completed" if any(char.isdigit() for char in joined) else "scheduled",
                                "scheduled_time": None,
                                "completed_time": None,
                                "alliance": None,
                                "opponent": joined,
                                "score_for": None,
                                "score_against": None,
                            }
                        )
        if focal_snapshot.get("rank") is None and division_rankings:
            focal_snapshot = None
        return {
            "snapshot": focal_snapshot,
            "matches": focal_matches,
            "division_rankings": division_rankings,
            "skills": skills_rows,
            "division_matches": division_matches,
            "tab_hits": sorted(tab_hits),
        }

    def parse_rankings_html(self, html_text: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Parse a rankings page fallback.

        This parser is intentionally narrow and may need selector adjustment if the
        event page layout changes.
        """
        parsed = self.parse_results_html(html_text)
        return parsed["snapshot"], parsed["matches"], parsed["division_rankings"], parsed["skills"]

    def _browser_click_targets(self) -> list[str]:
        """Return likely public tab labels for dynamic results content."""
        return [
            "#results-",
            "Rankings",
            "Qualifications",
            "Results",
            "Matches",
            "Skills",
            "Awards",
            self.settings.division_name,
        ]

    def _merge_browser_result(
        self,
        merged: dict[str, Any],
        parsed: dict[str, Any],
        *,
        source: str,
        result_tab: str,
        coverage: dict[str, list[str]],
    ) -> None:
        """Merge parsed browser content into the accumulated fallback payload."""
        snapshot = parsed["snapshot"]
        matches = parsed["matches"]
        division_rankings = parsed["division_rankings"]
        skills_rows = parsed["skills"]
        division_matches = parsed["division_matches"]
        if snapshot and merged["snapshot"] is None:
            snapshot["source"] = source
            snapshot["result_tab"] = result_tab
            merged["snapshot"] = snapshot
        if matches:
            coverage["matches"].append(result_tab)
            existing_keys = {item["match_key"] for item in merged["matches"]}
            for item in matches:
                if item["match_key"] in existing_keys:
                    continue
                item["source"] = source
                item["result_tab"] = result_tab
                merged["matches"].append(item)
                existing_keys.add(item["match_key"])
        if division_rankings:
            coverage["standings"].append(result_tab)
            existing_teams = {item["team_number"] for item in merged["division_rankings"]}
            for item in division_rankings:
                if item["team_number"] in existing_teams:
                    continue
                item["source"] = source
                item["result_tab"] = result_tab
                merged["division_rankings"].append(item)
                existing_teams.add(item["team_number"])
        if skills_rows:
            coverage["skills"].append(result_tab)
            existing_teams = {item["team_number"] for item in merged["skills"]}
            for item in skills_rows:
                if item["team_number"] in existing_teams:
                    continue
                item["source"] = source
                item["result_tab"] = result_tab
                merged["skills"].append(item)
                existing_teams.add(item["team_number"])
        if division_matches:
            coverage["division_matches"].append(result_tab)
            existing_keys = {item["match_key"] for item in merged["division_matches"]}
            for item in division_matches:
                if item["match_key"] in existing_keys:
                    continue
                item["source"] = source
                item["result_tab"] = result_tab
                merged["division_matches"].append(item)
                existing_keys.add(item["match_key"])

    def _browser_fallback_fetch(
        self, warnings: list[str]
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        """Render the public event page in a headless browser and parse the hydrated DOM."""
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            warnings.append("playwright is not installed, so browser fallback is unavailable")
            raise RuntimeError("Browser fallback unavailable") from exc

        merged: dict[str, Any] = {
            "snapshot": None,
            "matches": [],
            "division_rankings": [],
            "skills": [],
            "division_matches": [],
        }
        coverage: dict[str, list[str]] = {"standings": [], "skills": [], "matches": [], "division_matches": []}
        requests_seen: list[str] = []
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_default_timeout(self.settings.browser_timeout_seconds * 1000)
            page.on(
                "response",
                lambda response: requests_seen.append(response.url)
                if any(token in response.url.lower() for token in ("result", "rank", "skill", "match", "event", "division"))
                else None,
            )
            try:
                try:
                    page.goto(self._results_anchor_url(), wait_until="domcontentloaded")
                except PlaywrightTimeoutError:
                    warnings.append("Browser fallback timed out waiting for domcontentloaded; using partial DOM")
                page.wait_for_timeout(self.settings.browser_wait_after_click_ms)
                candidate_html: list[tuple[str, str]] = [("#results-", page.content())]
                for label in self._browser_click_targets():
                    if label == "#results-":
                        continue
                    locator = page.get_by_text(label, exact=False)
                    try:
                        if locator.count() < 1:
                            continue
                        locator.first.click(timeout=self.settings.browser_timeout_seconds * 1000)
                        page.wait_for_timeout(self.settings.browser_wait_after_click_ms)
                        candidate_html.append((label.lower(), page.content()))
                    except PlaywrightTimeoutError:
                        warnings.append(f"Browser fallback could not open '{label}' before timeout")
                    except Exception as exc:
                        warnings.append(f"Browser fallback click for '{label}' failed: {exc}")
                for result_tab, html_text in candidate_html:
                    self._merge_browser_result(
                        merged,
                        self.parse_results_html(html_text, source="results_tab_browser", result_tab=result_tab),
                        source="browser_fallback",
                        result_tab=result_tab,
                        coverage=coverage,
                    )
            finally:
                browser.close()
        return (
            merged["snapshot"],
            merged["matches"],
            merged["division_rankings"],
            merged["skills"],
            merged["division_matches"],
            {
                "attempted_tabs": self._browser_click_targets(),
                "successful_tabs": sorted({tab for values in coverage.values() for tab in values}),
                "dataset_tabs": {key: sorted(set(values)) for key, values in coverage.items()},
                "request_urls": sorted(set(requests_seen))[:25],
            },
        )

    def _fallback_fetch(self, warnings: list[str]) -> RobotEventsResult:
        """Fetch public standings from the event page when API access is unavailable."""
        response = self._request(self._fallback_url(), accept="text/html")
        response.raise_for_status()
        parsed = self.parse_results_html(response.text, source="html_fallback", result_tab="event_page")
        snapshot = parsed["snapshot"]
        matches = parsed["matches"]
        division_rankings = parsed["division_rankings"]
        skills = parsed["skills"]
        division_matches = parsed["division_matches"]
        snapshot_source = "html_fallback"
        result_tabs = {
            "attempted_tabs": ["event_page", "#results-"],
            "successful_tabs": parsed["tab_hits"],
            "dataset_tabs": {
                "standings": ["event_page"] if division_rankings else [],
                "skills": ["event_page"] if skills else [],
                "matches": ["event_page"] if matches else [],
                "division_matches": ["event_page"] if division_matches else [],
            },
            "request_urls": [],
        }
        if not division_rankings:
            if self.settings.enable_browser_fallback:
                warnings.append("Event page did not expose server-rendered standings; trying browser fallback")
                try:
                    snapshot, browser_matches, division_rankings, browser_skills, browser_division_matches, browser_tabs = self._browser_fallback_fetch(warnings)
                    if browser_matches:
                        matches = browser_matches
                    if browser_skills:
                        skills = browser_skills
                    if browser_division_matches:
                        division_matches = browser_division_matches
                    if division_rankings:
                        snapshot_source = "results_tab_browser"
                    result_tabs = browser_tabs
                except Exception as exc:
                    warnings.append(f"Browser fallback failed: {exc}")
            if not division_rankings:
                warnings.append("Browser and HTML fallbacks did not yield standings; loading division roster PDF instead")
                division_rankings = self._fetch_division_list_roster(warnings)
                snapshot_source = "division_list_pdf"
                result_tabs["successful_tabs"] = sorted(set(result_tabs["successful_tabs"] + ["division_list_pdf"]))
                result_tabs["dataset_tabs"]["standings"] = ["division_list_pdf"]
                if snapshot is None:
                    snapshot = next(
                        (
                            {
                                **item,
                                "school_name": item.get("organization") or self.settings.school_alias,
                            }
                            for item in division_rankings
                            if str(item.get("team_number", "")).lower() == self.settings.team_number.lower()
                        ),
                        None,
                    )
        if snapshot is None:
            warnings.append(f"Focal team {self.settings.team_number} not present in fallback standings")
        if not skills:
            warnings.append("Public fallback did not yield any skills rows")
        return RobotEventsResult(
            snapshot=snapshot,
            matches=matches,
            division_rankings=division_rankings,
            skills=skills,
            division_matches=division_matches,
            snapshot_source=snapshot_source,
            warnings=warnings,
            result_tabs=result_tabs,
        )

    def fetch(self) -> RobotEventsResult:
        """Fetch competition data with API-first behavior and HTML fallback."""
        warnings: list[str] = []
        if not self.settings.robotevents_api_key:
            warnings.append("ROBOTEVENTS_API_KEY not configured; using public fallback")
            return self._fallback_fetch(warnings)
        try:
            event_payload = self._fetch_event_payload()
            division_payload = self._find_division(event_payload)
            event_id = int(event_payload["id"])
            division_id = int(division_payload["id"])
            fetched_at = utc_now()
            rankings_payload = self._safe_api_get(
                f"/events/{event_id}/divisions/{division_id}/rankings",
                warning_label="rankings unavailable",
                warnings=warnings,
            )
            if not rankings_payload or not (rankings_payload.get("data") or []):
                warnings.append("API rankings unavailable; falling back to public standings page")
                return self._fallback_fetch(warnings)
            matches_payload = self._safe_api_get(
                f"/events/{event_id}/divisions/{division_id}/matches",
                warning_label="matches unavailable",
                warnings=warnings,
            )
            skills_payload = self._safe_api_get(
                f"/events/{event_id}/divisions/{division_id}/skills",
                warning_label="skills unavailable",
                warnings=warnings,
            )
            division_rankings = self._normalize_division_rankings(event_payload, division_payload, rankings_payload, fetched_at)
            try:
                snapshot = self._extract_focal_snapshot(division_rankings)
            except RuntimeError as exc:
                warnings.append(str(exc))
                snapshot = None
            division_matches = self._normalize_division_matches(matches_payload or {"data": []}, division_payload.get("name", self.settings.division_name))
            focal_matches = self._extract_focal_matches(division_matches)
            skills = self._normalize_skills(skills_payload or {"data": []}, division_payload.get("name", self.settings.division_name), fetched_at)
            return RobotEventsResult(
                snapshot=snapshot,
                matches=focal_matches,
                division_rankings=division_rankings,
                skills=skills,
                division_matches=division_matches,
                snapshot_source="api",
                warnings=warnings,
                result_tabs={
                    "attempted_tabs": ["api_rankings", "api_matches", "api_skills"],
                    "successful_tabs": ["api_rankings"] + (["api_matches"] if division_matches else []) + (["api_skills"] if skills else []),
                    "dataset_tabs": {
                        "standings": ["api_rankings"],
                        "skills": ["api_skills"] if skills else [],
                        "matches": ["api_matches"] if focal_matches else [],
                        "division_matches": ["api_matches"] if division_matches else [],
                    },
                    "request_urls": [],
                },
            )
        except Exception as exc:
            LOGGER.warning(
                "RobotEvents API failed, falling back to HTML parser",
                extra={"collector": "robotevents", "error": str(exc)},
            )
            warnings.append(f"API fetch failed: {exc}")
            return self._fallback_fetch(warnings)
