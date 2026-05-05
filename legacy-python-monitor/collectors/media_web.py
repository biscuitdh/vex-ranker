"""Targeted media and public web collector."""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import httpx

from config import Settings
from storage.db import utc_now

LOGGER = logging.getLogger(__name__)

PLATFORM_PRIORITY = {
    "official": 4,
    "reddit": 3,
    "instagram": 3,
    "tiktok": 3,
    "facebook": 3,
    "youtube": 3,
    "rss": 2,
    "web": 1,
    "social": 3,
}


def canonicalize_url(url: str) -> str:
    """Normalize a URL for dedupe purposes."""
    parsed = urlparse(url)
    hostname = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme.lower()}://{hostname}{path}"


def classify_confidence(url: str) -> str:
    """Classify a source by hostname."""
    host = urlparse(url).netloc.lower()
    if any(domain in host for domain in ("robotevents.com", "recf.org", "vexrobotics.com", "news.vex.com")):
        return "official"
    if any(domain in host for domain in ("youtube.com", "youtu.be", "reddit.com", "facebook.com", "instagram.com", "tiktok.com", ".edu", ".k12.", ".org", ".gov")):
        return "trusted"
    return "unverified"


def source_type_for_platform(platform: str) -> str:
    """Map a platform to a source type."""
    if platform in {"reddit", "instagram", "tiktok", "facebook", "youtube"}:
        return "social"
    if platform == "official":
        return "official"
    if platform == "rss":
        return "community"
    return "web"


def author_from_url(url: str) -> str:
    """Extract a best-effort author handle from a URL path."""
    parsed = urlparse(url)
    bits = [bit for bit in parsed.path.split("/") if bit]
    if not bits:
        return ""
    if parsed.netloc.endswith("reddit.com") and len(bits) > 1 and bits[0] in {"r", "u", "user"}:
        return bits[1]
    return bits[0]


class MediaWebCollector:
    """Collect targeted web and media mentions without crawling the internet blindly."""

    def __init__(self, settings: Settings, client: httpx.Client | None = None) -> None:
        self.settings = settings
        self.last_failures: list[str] = []
        self.client = client or httpx.Client(
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "vex-ranker-monitor/1.0"},
        )
        self._managed_client = client is None

    def close(self) -> None:
        """Close the owned HTTP client."""
        if self._managed_client:
            self.client.close()

    def _request(self, url: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        """Perform an HTTP request with retry and backoff."""
        last_error: Exception | None = None
        for attempt in range(1, self.settings.http_max_retries + 1):
            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                return response
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_error = exc
                LOGGER.warning(
                    "Media request failed",
                    extra={"collector": "media_web", "error": str(exc), "source": url},
                )
                if attempt >= self.settings.http_max_retries:
                    break
                time.sleep(self.settings.http_backoff_base_seconds ** attempt)
        raise RuntimeError(f"Media request failed after retries: {last_error}") from last_error

    def _build_item(
        self,
        *,
        title: str,
        url: str,
        source: str,
        query_term: str,
        collector_name: str,
        platform: str,
        snippet: str = "",
        published_at: str | None = None,
        matched_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """Normalize one media item."""
        canonical = canonicalize_url(url)
        return {
            "canonical_key": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            "title": title.strip() or canonical,
            "url": url,
            "source": source,
            "snippet": snippet.strip(),
            "published_at": published_at,
            "discovered_at": utc_now(),
            "confidence": classify_confidence(url),
            "query_term": query_term,
            "platform": platform,
            "source_type": source_type_for_platform(platform),
            "author_handle": author_from_url(url),
            "matched_terms": matched_terms or [query_term],
            "collector_name": collector_name,
        }

    def _google_news_items(self, query_term: str) -> list[dict[str, Any]]:
        """Collect search results from Google News RSS."""
        response = self._request(
            "https://news.google.com/rss/search",
            params={"q": query_term, "hl": "en-US", "gl": "US", "ceid": "US:en"},
        )
        root = ET.fromstring(response.content)
        items: list[dict[str, Any]] = []
        for node in root.findall(".//item"):
            title = node.findtext("title", default="")
            link = node.findtext("link", default="")
            pub_date = node.findtext("pubDate", default="")
            source = node.findtext("source", default="Google News")
            description = node.findtext("description", default="")
            if link:
                items.append(
                    self._build_item(
                        title=title,
                        url=link,
                        source=source or "Google News",
                        query_term=query_term,
                        collector_name="google_news",
                        platform="web",
                        snippet=description,
                        published_at=pub_date,
                    )
                )
        return items

    def _duckduckgo_items(
        self,
        query_term: str,
        *,
        source_tag: str,
        collector_name: str,
        platform: str,
        matched_terms: list[str] | None = None,
        allowed_domains: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Collect HTML search results from DuckDuckGo."""
        response = self._request("https://html.duckduckgo.com/html/", params={"q": query_term})
        soup = BeautifulSoup(response.text, "html.parser")
        items: list[dict[str, Any]] = []
        for result in soup.select(".result"):
            anchor = result.select_one(".result__title a")
            snippet_node = result.select_one(".result__snippet")
            if not anchor or not anchor.get("href"):
                continue
            href = anchor["href"]
            parsed = urlparse(href)
            if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
                href = unquote(parse_qs(parsed.query).get("uddg", [href])[0])
            resolved_host = urlparse(href).netloc.lower()
            if allowed_domains and not any(domain in resolved_host for domain in allowed_domains):
                continue
            title = anchor.get_text(" ", strip=True)
            snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
            items.append(
                self._build_item(
                    title=title,
                    url=href,
                    source=source_tag,
                    query_term=query_term,
                    collector_name=collector_name,
                    platform=platform,
                    snippet=snippet,
                    matched_terms=matched_terms,
                )
            )
        return items

    def _seed_url_items(self, urls: list[str], *, platform: str) -> list[dict[str, Any]]:
        """Collect direct items from configured seed URLs."""
        items: list[dict[str, Any]] = []
        for url in urls:
            try:
                response = self._request(url)
            except RuntimeError:
                continue
            soup = BeautifulSoup(response.text, "html.parser")
            title_node = soup.find("meta", attrs={"property": "og:title"}) or soup.find("title")
            desc_node = soup.find("meta", attrs={"property": "og:description"}) or soup.find("meta", attrs={"name": "description"})
            title = title_node.get("content") if title_node and title_node.has_attr("content") else title_node.get_text(" ", strip=True) if title_node else url
            snippet = desc_node.get("content") if desc_node and desc_node.has_attr("content") else ""
            items.append(
                self._build_item(
                    title=title,
                    url=url,
                    source=urlparse(url).netloc or "Seed Source",
                    query_term=url,
                    collector_name="seed_url",
                    platform=platform,
                    snippet=snippet,
                    matched_terms=[],
                )
            )
        return items

    def _official_source_items(self, query_term: str) -> list[dict[str, Any]]:
        """Collect official-source search results."""
        source_query = f'{query_term} site:robotevents.com OR site:recf.org OR site:vexrobotics.com OR site:news.vex.com'
        return self._duckduckgo_items(
            source_query,
            source_tag="Official Search",
            collector_name="official_search",
            platform="official",
            matched_terms=[query_term],
            allowed_domains=["robotevents.com", "recf.org", "vexrobotics.com", "news.vex.com"],
        )

    def _community_source_items(self, query_term: str) -> list[dict[str, Any]]:
        """Collect school, community, and district results."""
        community_query = f'{query_term} site:.edu OR site:.org OR site:.k12.ny.us OR site:.gov'
        return self._duckduckgo_items(
            community_query,
            source_tag="Community Search",
            collector_name="community_search",
            platform="web",
            matched_terms=[query_term],
            allowed_domains=[".edu", ".org", ".k12.", ".gov"],
        )

    def _platform_items(self, query_term: str, site_filter: str, platform: str) -> list[dict[str, Any]]:
        """Collect one platform using site-filtered search."""
        return self._duckduckgo_items(
            f"{query_term} site:{site_filter}",
            source_tag=f"{platform.title()} Search",
            collector_name=f"{platform}_search",
            platform=platform,
            matched_terms=[query_term],
            allowed_domains=[site_filter],
        )

    def _rss_items(self, rss_url: str) -> list[dict[str, Any]]:
        """Collect items from an optional RSS source."""
        response = self._request(rss_url)
        root = ET.fromstring(response.content)
        items: list[dict[str, Any]] = []
        for node in root.findall(".//item"):
            title = node.findtext("title", default="")
            link = node.findtext("link", default="")
            if not link:
                continue
            items.append(
                self._build_item(
                    title=title,
                    url=link,
                    source=urlparse(rss_url).netloc or "RSS",
                    query_term=rss_url,
                    collector_name="rss",
                    platform="rss",
                    snippet=node.findtext("description", default=""),
                    published_at=node.findtext("pubDate", default=""),
                    matched_terms=[],
                )
            )
        return items

    def fetch(self) -> list[dict[str, Any]]:
        """Collect targeted media items from enabled sources."""
        items: list[dict[str, Any]] = []
        self.last_failures = []

        def _extend_safely(label: str, func, *args, **kwargs) -> None:
            try:
                items.extend(func(*args, **kwargs))
            except RuntimeError as exc:
                self.last_failures.append(f"{label}: {exc}")
                LOGGER.warning(
                    "Media source failed but collection will continue",
                    extra={"collector": "media_web", "source": label, "error": str(exc)},
                )

        for term in self.settings.search_terms:
            _extend_safely(f"google_news:{term}", self._google_news_items, term)
            _extend_safely(
                f"web_search:{term}",
                self._duckduckgo_items,
                term,
                source_tag="Web Search",
                collector_name="web_search",
                platform="web",
            )
            _extend_safely(f"community_search:{term}", self._community_source_items, term)
            if self.settings.enable_official_sources:
                _extend_safely(f"official_search:{term}", self._official_source_items, term)

            if self.settings.enable_optional_social:
                if self.settings.enable_reddit:
                    _extend_safely(f"reddit:{term}", self._platform_items, term, "reddit.com", "reddit")
                if self.settings.enable_instagram:
                    _extend_safely(f"instagram:{term}", self._platform_items, term, "instagram.com", "instagram")
                if self.settings.enable_tiktok:
                    _extend_safely(f"tiktok:{term}", self._platform_items, term, "tiktok.com", "tiktok")
                if self.settings.enable_facebook:
                    _extend_safely(f"facebook:{term}", self._platform_items, term, "facebook.com", "facebook")
                if self.settings.enable_youtube:
                    _extend_safely(f"youtube:{term}", self._platform_items, term, "youtube.com", "youtube")
            elif self.settings.enable_youtube:
                _extend_safely(f"youtube:{term}", self._platform_items, term, "youtube.com", "youtube")

        if self.settings.enable_rss_sources:
            for rss_url in self.settings.optional_rss_urls:
                _extend_safely(f"rss:{rss_url}", self._rss_items, rss_url)

        if self.settings.enable_official_sources:
            items.extend(self._seed_url_items(self.settings.official_source_urls, platform="official"))
        items.extend(self._seed_url_items(self.settings.community_source_urls, platform="web"))
        items.extend(self._seed_url_items(self.settings.school_source_urls, platform="web"))
        if self.settings.enable_optional_social:
            items.extend(self._seed_url_items(self.settings.social_seed_urls, platform="social"))

        deduped: dict[str, dict[str, Any]] = {}
        for item in items:
            existing = deduped.get(item["canonical_key"])
            if existing is None:
                deduped[item["canonical_key"]] = item
                continue
            existing_priority = PLATFORM_PRIORITY.get(existing.get("platform", "web"), 0)
            new_priority = PLATFORM_PRIORITY.get(item.get("platform", "web"), 0)
            if new_priority > existing_priority:
                deduped[item["canonical_key"]] = item
        return list(deduped.values())
