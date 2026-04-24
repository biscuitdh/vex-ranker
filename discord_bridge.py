"""Discord bridge service entrypoint."""

from __future__ import annotations

import logging

from config import load_settings
from notify.discord_bridge import discord_configuration_issues, run_bridge_loop
from utils.logging import configure_logging

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Run the Discord bridge service."""
    settings = load_settings()
    configure_logging(settings.log_dir, settings.log_level)
    for issue in discord_configuration_issues(settings):
        LOGGER.warning(issue)
    run_bridge_loop(settings)


if __name__ == "__main__":
    main()
