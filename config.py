"""Application configuration for the VEX monitoring agent."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
from typing import Iterable

from dotenv import load_dotenv


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean-like environment value."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    """Parse an integer environment value with a safe default."""
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_float(value: str | None, default: float) -> float:
    """Parse a float environment value with a safe default."""
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _parse_terms(value: str | None) -> list[str]:
    """Parse a comma or newline delimited list."""
    if not value:
        return []
    parts = [piece.strip() for chunk in value.splitlines() for piece in chunk.split(",")]
    return [piece for piece in parts if piece]


@dataclass(slots=True)
class Settings:
    """Runtime configuration loaded from the environment."""

    base_dir: Path
    data_dir: Path
    reports_dir: Path
    log_dir: Path
    db_path: Path
    static_site_dir: Path
    log_level: str = "INFO"
    timezone: str = "America/New_York"
    robotevents_api_key: str = ""
    robotevents_api_base: str = "https://www.robotevents.com/api/v2"
    discord_webhook_url: str = ""
    discord_bot_token: str = ""
    discord_application_id: str = ""
    discord_public_key: str = ""
    discord_channel_id: str = ""
    discord_allowed_user_ids: list[str] = field(default_factory=list)
    discord_reply_timeout_minutes: int = 20
    discord_approval_prefix: str = "approve"
    discord_text_fallback_enabled: bool = False
    poll_interval_minutes: int = 10
    media_interval_minutes: int = 60
    enable_background_media_watcher: bool = True
    daily_summary_hour: int = 20
    event_sku: str = "RE-V5RC-26-4025"
    division_name: str = "Technology"
    team_number: str = "7157B"
    team_name_alias: str = "Mystery Machine"
    school_alias: str = "Chittenango High School, NY"
    event_name_alias: str = "2026 VEX Robotics World Championship"
    livestream_url: str = (
        "https://www.vexworlds.tv/#/viewer/broadcasts/"
        "practice--qualification-matches-technology-mv6olnh0lcdsjnediguv/"
        "xponhawezq7adhmfdycu"
    )
    request_timeout_seconds: int = 20
    http_max_retries: int = 3
    http_backoff_base_seconds: int = 2
    http_rate_limit_per_minute: int = 30
    enable_browser_fallback: bool = False
    browser_timeout_seconds: int = 30
    browser_wait_after_click_ms: int = 1500
    enable_vexvia_local: bool = True
    vexvia_container_path: Path | None = None
    vexvia_event_db_path: Path | None = None
    vexvia_skills_db_path: Path | None = None
    media_confidence_notify_min: str = "unverified"
    enable_optional_social: bool = False
    enable_reddit: bool = True
    enable_instagram: bool = False
    enable_tiktok: bool = False
    enable_facebook: bool = False
    enable_youtube: bool = True
    enable_official_sources: bool = True
    enable_rss_sources: bool = True
    gui_host: str = "127.0.0.1"
    gui_port: int = 8787
    static_site_base_url: str = ""
    github_pages_repo: Path | None = None
    git_push_enabled: bool = False
    publish_branch: str = "main"
    healthcheck_interval_minutes: int = 60
    dashboard_stale_minutes: int = 75
    ai_rankings_stale_minutes: int = 90
    match_progress_grace_minutes: int = 10
    max_auto_repair_attempts: int = 2
    restart_cooldown_minutes: int = 30
    enable_auto_heal: bool = True
    enable_service_restart: bool = True
    backend_service_label: str = "com.vexranker.monitor"
    gui_service_label: str = "com.vexranker.gui"
    power_rank_recent_match_count: int = 5
    power_rank_weight_official: float = 0.35
    power_rank_weight_opr: float = 0.20
    power_rank_weight_dpr: float = 0.10
    power_rank_weight_ccwm: float = 0.15
    power_rank_weight_skills: float = 0.10
    power_rank_weight_form: float = 0.10
    power_rank_weight_manual: float = 0.12
    search_terms: list[str] = field(default_factory=list)
    optional_rss_urls: list[str] = field(default_factory=list)
    official_source_urls: list[str] = field(default_factory=list)
    community_source_urls: list[str] = field(default_factory=list)
    school_source_urls: list[str] = field(default_factory=list)
    social_seed_urls: list[str] = field(default_factory=list)

    @property
    def notify_confidence_rank(self) -> int:
        """Return a sortable confidence threshold."""
        mapping = {"unverified": 0, "trusted": 1, "official": 2}
        return mapping.get(self.media_confidence_notify_min.lower(), 0)


def ensure_directories(paths: Iterable[Path]) -> None:
    """Create required runtime directories."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _default_search_terms() -> list[str]:
    """Return the default targeted search terms."""
    return [
        "7157B",
        "7157 B",
        "Mystery Machine",
        '"Mystery Machine" VEX',
        '"7157B" VEX',
        '"7157B" Worlds',
        '"Chittenango" VEX',
        '"Chittenango Robotics"',
        '"RE-V5RC-26-4025"',
        '"2026 VEX Robotics World Championship" Technology division 7157B',
    ]


def _default_official_sources() -> list[str]:
    """Return default official source URLs."""
    return [
        "https://www.robotevents.com/robot-competitions/vex-robotics-competition/RE-V5RC-26-4025.html",
        "https://www.robotevents.com/robot-competitions/vex-robotics-competition/RE-V5RC-26-4025.html#results-",
        "https://www.robotevents.com/api/v2",
        "https://recf.org/vex_worlds/division-lists/",
        "https://news.vex.com/",
    ]


def load_settings(env_file: str | None = ".env") -> Settings:
    """Load application settings from the local environment."""
    if env_file:
        load_dotenv(env_file)
    base_dir = Path(os.getenv("BASE_DIR", Path.cwd())).resolve()
    data_dir = (base_dir / os.getenv("DATA_DIR", "data")).resolve()
    reports_dir = (base_dir / os.getenv("REPORTS_DIR", "reports")).resolve()
    log_dir = (base_dir / os.getenv("LOG_DIR", "logs")).resolve()
    db_path = (base_dir / os.getenv("DB_PATH", "data/monitor.db")).resolve()
    static_site_dir = (base_dir / os.getenv("STATIC_SITE_DIR", "site")).resolve()

    settings = Settings(
        base_dir=base_dir,
        data_dir=data_dir,
        reports_dir=reports_dir,
        log_dir=log_dir,
        db_path=db_path,
        static_site_dir=static_site_dir,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        timezone=os.getenv("TIMEZONE", "America/New_York"),
        robotevents_api_key=os.getenv("ROBOTEVENTS_API_KEY", "").strip(),
        robotevents_api_base=os.getenv("ROBOTEVENTS_API_BASE", "https://www.robotevents.com/api/v2").rstrip("/"),
        discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        discord_bot_token=os.getenv("DISCORD_BOT_TOKEN", "").strip(),
        discord_application_id=os.getenv("DISCORD_APPLICATION_ID", "").strip(),
        discord_public_key=os.getenv("DISCORD_PUBLIC_KEY", "").strip(),
        discord_channel_id=os.getenv("DISCORD_CHANNEL_ID", "").strip(),
        discord_allowed_user_ids=_parse_terms(os.getenv("DISCORD_ALLOWED_USER_IDS")),
        discord_reply_timeout_minutes=max(1, _parse_int(os.getenv("DISCORD_REPLY_TIMEOUT_MINUTES"), 20)),
        discord_approval_prefix=os.getenv("DISCORD_APPROVAL_PREFIX", "approve").strip().lower() or "approve",
        discord_text_fallback_enabled=_parse_bool(os.getenv("DISCORD_TEXT_FALLBACK_ENABLED"), False),
        poll_interval_minutes=_parse_int(os.getenv("POLL_INTERVAL_MINUTES"), 10),
        media_interval_minutes=_parse_int(os.getenv("MEDIA_INTERVAL_MINUTES"), 60),
        enable_background_media_watcher=_parse_bool(os.getenv("ENABLE_BACKGROUND_MEDIA_WATCHER"), True),
        daily_summary_hour=max(0, min(23, _parse_int(os.getenv("DAILY_SUMMARY_HOUR"), 20))),
        event_sku=os.getenv("EVENT_SKU", "RE-V5RC-26-4025").strip(),
        division_name=os.getenv("DIVISION_NAME", "Technology").strip(),
        team_number=os.getenv("TEAM_NUMBER", "7157B").strip(),
        team_name_alias=os.getenv("TEAM_NAME_ALIAS", "Mystery Machine").strip(),
        school_alias=os.getenv("SCHOOL_ALIAS", "Chittenango High School, NY").strip(),
        event_name_alias=os.getenv("EVENT_NAME_ALIAS", "2026 VEX Robotics World Championship").strip(),
        livestream_url=os.getenv(
            "LIVESTREAM_URL",
            (
                "https://www.vexworlds.tv/#/viewer/broadcasts/"
                "practice--qualification-matches-technology-mv6olnh0lcdsjnediguv/"
                "xponhawezq7adhmfdycu"
            ),
        ).strip(),
        request_timeout_seconds=_parse_int(os.getenv("REQUEST_TIMEOUT_SECONDS"), 20),
        http_max_retries=_parse_int(os.getenv("HTTP_MAX_RETRIES"), 3),
        http_backoff_base_seconds=max(0, _parse_int(os.getenv("HTTP_BACKOFF_BASE_SECONDS"), 2)),
        http_rate_limit_per_minute=max(1, _parse_int(os.getenv("HTTP_RATE_LIMIT_PER_MINUTE"), 30)),
        enable_browser_fallback=_parse_bool(os.getenv("ENABLE_BROWSER_FALLBACK"), False),
        browser_timeout_seconds=max(5, _parse_int(os.getenv("BROWSER_TIMEOUT_SECONDS"), 30)),
        browser_wait_after_click_ms=max(250, _parse_int(os.getenv("BROWSER_WAIT_AFTER_CLICK_MS"), 1500)),
        enable_vexvia_local=_parse_bool(os.getenv("ENABLE_VEXVIA_LOCAL"), True),
        vexvia_container_path=Path(os.getenv("VEXVIA_CONTAINER_PATH")).expanduser().resolve()
        if os.getenv("VEXVIA_CONTAINER_PATH")
        else None,
        vexvia_event_db_path=Path(os.getenv("VEXVIA_EVENT_DB_PATH")).expanduser().resolve()
        if os.getenv("VEXVIA_EVENT_DB_PATH")
        else None,
        vexvia_skills_db_path=Path(os.getenv("VEXVIA_SKILLS_DB_PATH")).expanduser().resolve()
        if os.getenv("VEXVIA_SKILLS_DB_PATH")
        else None,
        media_confidence_notify_min=os.getenv("MEDIA_CONFIDENCE_NOTIFY_MIN", "unverified").strip().lower(),
        enable_optional_social=_parse_bool(os.getenv("ENABLE_OPTIONAL_SOCIAL"), False),
        enable_reddit=_parse_bool(os.getenv("ENABLE_REDDIT"), True),
        enable_instagram=_parse_bool(os.getenv("ENABLE_INSTAGRAM"), False),
        enable_tiktok=_parse_bool(os.getenv("ENABLE_TIKTOK"), False),
        enable_facebook=_parse_bool(os.getenv("ENABLE_FACEBOOK"), False),
        enable_youtube=_parse_bool(os.getenv("ENABLE_YOUTUBE"), True),
        enable_official_sources=_parse_bool(os.getenv("ENABLE_OFFICIAL_SOURCES"), True),
        enable_rss_sources=_parse_bool(os.getenv("ENABLE_RSS_SOURCES"), True),
        gui_host=os.getenv("GUI_HOST", "127.0.0.1").strip(),
        gui_port=_parse_int(os.getenv("GUI_PORT"), 8787),
        static_site_base_url=os.getenv("STATIC_SITE_BASE_URL", "").strip().rstrip("/"),
        github_pages_repo=Path(os.getenv("GITHUB_PAGES_REPO")).expanduser().resolve()
        if os.getenv("GITHUB_PAGES_REPO")
        else None,
        git_push_enabled=_parse_bool(os.getenv("GIT_PUSH_ENABLED"), False),
        publish_branch=os.getenv("PUBLISH_BRANCH", "main").strip() or "main",
        healthcheck_interval_minutes=max(5, _parse_int(os.getenv("HEALTHCHECK_INTERVAL_MINUTES"), 60)),
        dashboard_stale_minutes=max(5, _parse_int(os.getenv("DASHBOARD_STALE_MINUTES"), 75)),
        ai_rankings_stale_minutes=max(5, _parse_int(os.getenv("AI_RANKINGS_STALE_MINUTES"), 90)),
        match_progress_grace_minutes=max(1, _parse_int(os.getenv("MATCH_PROGRESS_GRACE_MINUTES"), 10)),
        max_auto_repair_attempts=max(1, _parse_int(os.getenv("MAX_AUTO_REPAIR_ATTEMPTS"), 2)),
        restart_cooldown_minutes=max(1, _parse_int(os.getenv("RESTART_COOLDOWN_MINUTES"), 30)),
        enable_auto_heal=_parse_bool(os.getenv("ENABLE_AUTO_HEAL"), True),
        enable_service_restart=_parse_bool(os.getenv("ENABLE_SERVICE_RESTART"), True),
        backend_service_label=os.getenv("BACKEND_SERVICE_LABEL", "com.vexranker.monitor").strip() or "com.vexranker.monitor",
        gui_service_label=os.getenv("GUI_SERVICE_LABEL", "com.vexranker.gui").strip() or "com.vexranker.gui",
        power_rank_recent_match_count=max(1, _parse_int(os.getenv("POWER_RANK_RECENT_MATCH_COUNT"), 5)),
        power_rank_weight_official=_parse_float(os.getenv("POWER_RANK_WEIGHT_OFFICIAL"), 0.35),
        power_rank_weight_opr=_parse_float(os.getenv("POWER_RANK_WEIGHT_OPR"), 0.20),
        power_rank_weight_dpr=_parse_float(os.getenv("POWER_RANK_WEIGHT_DPR"), 0.10),
        power_rank_weight_ccwm=_parse_float(os.getenv("POWER_RANK_WEIGHT_CCWM"), 0.15),
        power_rank_weight_skills=_parse_float(os.getenv("POWER_RANK_WEIGHT_SKILLS"), 0.10),
        power_rank_weight_form=_parse_float(os.getenv("POWER_RANK_WEIGHT_FORM"), 0.10),
        power_rank_weight_manual=min(0.15, max(0.0, _parse_float(os.getenv("POWER_RANK_WEIGHT_MANUAL"), 0.12))),
        search_terms=_parse_terms(os.getenv("SEARCH_TERMS")) or _default_search_terms(),
        optional_rss_urls=_parse_terms(os.getenv("OPTIONAL_RSS_URLS")),
        official_source_urls=_parse_terms(os.getenv("OFFICIAL_SOURCE_URLS")) or _default_official_sources(),
        community_source_urls=_parse_terms(os.getenv("COMMUNITY_SOURCE_URLS")),
        school_source_urls=_parse_terms(os.getenv("SCHOOL_SOURCE_URLS")),
        social_seed_urls=_parse_terms(os.getenv("SOCIAL_SEED_URLS")),
    )
    ensure_directories((settings.data_dir, settings.reports_dir, settings.log_dir, settings.static_site_dir))
    return settings
