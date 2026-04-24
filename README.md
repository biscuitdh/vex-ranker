# VEX Worlds 2026 Monitoring Agent

Lightweight monitoring agent for VEX Robotics Team `7157B` ("Mystery Machine", Chittenango High School, NY) at the `2026 VEX Robotics World Championship`, with official competition tracking, division-wide rankings context, targeted media discovery, optional social adapters, SQLite-backed history, Markdown/JSON reporting, a local browser GUI, and a static GitHub Pages export path updated from your Mac.

## Features

- Team-first dashboard focused on `7157B`
- Division-wide official standings and skills ingestion for the configured division
- Derived `Power Rank` model using official standing context, OPR, DPR, CCWM, skills, and recent form
- RobotEvents API v2 first, with narrow public fallbacks: VEX Via local cache, static HTML, optional Playwright browser rendering for the official event page, then RECF division-list PDF roster bootstrap
- Broader media collection: Google News RSS, DuckDuckGo HTML search, official-source search, community/school search, optional RSS, and optional social adapters
- SQLite history for focal-team snapshots, division standings, skills, division matches, derived metrics, media items, collector runs, and alert dedupe
- Structured JSON logging to console and `logs/monitor.log`
- Markdown and JSON report export to `reports/`
- Browser-only local GUI with dashboard, rankings, matches, media, history, and settings views
- Autonomous analysis layer with an Analysis tab and summary commentary derived from current stored data
- Static site export to `site/` for GitHub Pages publishing
- Optional publish-to-repo workflow for a separate checked-out Pages repo
- Discord webhook alerts plus an optional private-channel approval bridge for away-from-Mac control

## Requirements

- Python 3.11+
- Network access to RobotEvents and selected public sources

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set at least:

- `ROBOTEVENTS_API_KEY`
- `EVENT_SKU`
- `DIVISION_NAME`
- `TEAM_NUMBER`

Optional:

- `DISCORD_WEBHOOK_URL`
- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`
- `DISCORD_ALLOWED_USER_IDS`
- `OPTIONAL_RSS_URLS`
- `SEARCH_TERMS`
- `ENABLE_OPTIONAL_SOCIAL=true`
- `ENABLE_BACKGROUND_MEDIA_WATCHER=true`
- `ENABLE_BROWSER_FALLBACK=true`
- `ENABLE_VEXVIA_LOCAL=true`
- `STATIC_SITE_DIR=site`
- `GITHUB_PAGES_REPO=/absolute/path/to/checked-out-pages-repo`
- `GIT_PUSH_ENABLED=true`

## Running Locally

One-shot collection:

```bash
python main.py --once
```

One-shot competition only:

```bash
python main.py --once --collector robotevents
```

One-shot dashboard health check:

```bash
python main.py --once --collector healthcheck
```

One-shot self-heal cycle:

```bash
python main.py --once --collector self_heal
```

Scheduler mode:

```bash
python main.py
```

Static export and optional publish:

```bash
python main.py --publish-static
```

GUI:

```bash
python gui_app.py
```

Then open [http://127.0.0.1:8787](http://127.0.0.1:8787).

The GUI now:

- opens immediately with stored data
- runs one background startup refresh for competition and AI rankings
- runs a separate background media watcher when `ENABLE_BACKGROUND_MEDIA_WATCHER=true`

The background media watcher uses `MEDIA_INTERVAL_MINUTES` and is limited to public mention discovery rather than standings collection.

## Static GitHub Pages Publishing

The clean remote-sharing path is now a static exported site:

1. your Mac runs the collectors locally
2. SQLite is updated from the freshest available local sources
3. the app exports HTML and JSON snapshots into `STATIC_SITE_DIR`
4. those generated files are copied into a separate checked-out Pages repo
5. git commit/push publishes the updated snapshot site

The static site includes:

- `index.html`
- `rankings/index.html`
- `ai-rankings/index.html`
- `matches/index.html`
- `media/index.html`
- `history/index.html`
- `settings/index.html`
- JSON snapshots under `site/data/`

The public static site is intentionally read-only:

- no manual refresh buttons
- no startup-refresh banners
- no local process controls
- no local file paths or secret config values

Recommended `.env` additions:

```dotenv
STATIC_SITE_DIR=site
GITHUB_PAGES_REPO=/Users/youruser/Documents/vex-ranker-pages
GIT_PUSH_ENABLED=true
PUBLISH_BRANCH=main
```

Notes:

- `GITHUB_PAGES_REPO` should be a **separate local git checkout** for the Pages site.
- This project folder does not need to be the Pages repo.
- If `GIT_PUSH_ENABLED=false`, `python main.py --publish-static` will still refresh data and generate the site locally without pushing.
- The exporter writes `.nojekyll` so GitHub Pages does not get clever in the wrong way.

### Suggested Pages Repo Layout

- Keep this repo for source code and local runtime
- Keep a second repo for generated Pages output only
- Configure GitHub Pages on that second repo

That split keeps generated artifacts out of the working codebase and makes rollback less annoying.

## Discord Setup

The repo now supports two Discord paths:

1. `DISCORD_WEBHOOK_URL` for passive alerts
2. a private-channel bot bridge for remote questions, button approvals, and explicit restart approvals

### Webhook Only

1. create a private Discord channel
2. create a channel webhook for that channel
3. paste the webhook into `DISCORD_WEBHOOK_URL`
4. test it with:

```bash
python main.py --once --collector healthcheck
```

If the webhook is missing or malformed, startup logging and dashboard health will call that out directly.

### Interactive Bridge

Add these `.env` values:

```dotenv
DISCORD_BOT_TOKEN=your_discord_bot_token
DISCORD_APPLICATION_ID=your_discord_application_id
DISCORD_PUBLIC_KEY=your_discord_public_key
DISCORD_CHANNEL_ID=123456789012345678
DISCORD_ALLOWED_USER_IDS=123456789012345678
DISCORD_REPLY_TIMEOUT_MINUTES=20
DISCORD_APPROVAL_PREFIX=approve
DISCORD_TEXT_FALLBACK_ENABLED=false
```

The bridge only trusts actions from `DISCORD_ALLOWED_USER_IDS` inside the configured channel. Normal operation is button-first:

- `Approve`
- `Deny`
- `Need Info`

Text replies still exist as an explicit fallback when `DISCORD_TEXT_FALLBACK_ENABLED=true`:

- `approve <request_id>`
- `deny <request_id>`
- `answer <request_id>: <text>`

Run the bridge locally with:

```bash
python discord_bridge.py
```

## Hourly Mac Updates

The repo now includes sample LaunchAgent plists for both long-running services:

`ops/com.vexranker.monitor.plist.example`
`ops/com.vexranker.gui.plist.example`
`ops/com.vexranker.discord-bridge.plist.example`

Recommended flow:

1. copy them to:
   `~/Library/LaunchAgents/com.vexranker.monitor.plist`
   `~/Library/LaunchAgents/com.vexranker.gui.plist`
2. replace the placeholder paths with your real project paths
3. load them with:

```bash
launchctl load ~/Library/LaunchAgents/com.vexranker.monitor.plist
launchctl load ~/Library/LaunchAgents/com.vexranker.gui.plist
launchctl load ~/Library/LaunchAgents/com.vexranker.discord-bridge.plist
```

The monitor agent runs:

```bash
/path/to/.venv/bin/python /path/to/main.py
```

The GUI agent runs:

```bash
/path/to/.venv/bin/python /path/to/gui_app.py
```

The internal self-heal loop checks freshness every `HEALTHCHECK_INTERVAL_MINUTES`, retries repairs automatically, and can kick both LaunchAgents through `launchctl kickstart -k` when the dashboard stays unhealthy.

## Optional Social Adapters

`ENABLE_OPTIONAL_SOCIAL=false` keeps the default collector conservative.

To enable broader social coverage:

```dotenv
ENABLE_OPTIONAL_SOCIAL=true
ENABLE_REDDIT=true
ENABLE_INSTAGRAM=true
ENABLE_TIKTOK=true
ENABLE_FACEBOOK=true
ENABLE_YOUTUBE=true
```

Use `SOCIAL_SEED_URLS` for known pages or communities you care about. X/Twitter remains out of scope by default.

## Rankings Page

The GUI now includes `/rankings` with:

- official division standings
- skills standings
- computed power rankings
- focal-team spotlight for `7157B`
- biggest movers between the latest two ranking snapshots

Official standings remain canonical. `Power Rank` is derived and clearly labeled as such.

## Analysis Page

The GUI includes `/analysis`, which automatically summarizes:

- team outlook
- division context
- rank and power movement
- collector/source health
- media signal

This layer is intentionally heuristic and transparent. It explains the current state using the data in SQLite, even when live official standings are only partially available.

## Autonomous Scouting Views

The GUI is now focused on autonomous collection and scouting context rather than manual imports. In addition to standings and power views, it includes:

- threat list for teams most relevant to `7157B`
- swing-match view for upcoming matches most likely to move the team's trajectory
- partner impact and opponent pressure tables derived from completed matches
- source freshness and collector health so the dashboard tells the truth about what it knows

## RobotEvents API Notes

The collector is written API-first, but RobotEvents endpoint shapes and filters can shift. These spots are intentionally commented and may need adjustment:

- event lookup by `sku[]`
- division rankings path
- division matches path
- division skills path

If the API does not return the expected fields, the collector falls back to a narrow HTML parser for event-specific pages instead of dragging in a full browser stack.

## Public No-Key Fallbacks

When `ROBOTEVENTS_API_KEY` is not configured, the collector uses public sources in this order:

1. local VEX Via cache on macOS when `ENABLE_VEXVIA_LOCAL=true`
2. static RobotEvents event-page HTML
3. optional browser-rendered RobotEvents event page when `ENABLE_BROWSER_FALLBACK=true`
4. RECF division-list PDF roster bootstrap

## VEX Via Local Cache

If the VEX Via app is installed on the same Mac, the monitor can read the app's local SQLite cache for standings, matches, skills, and ranking freshness without needing RobotEvents API access.

Defaults:

```dotenv
ENABLE_VEXVIA_LOCAL=true
```

Optional explicit paths:

```dotenv
VEXVIA_CONTAINER_PATH=/Users/youruser/Library/Containers/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
VEXVIA_EVENT_DB_PATH=/Users/youruser/Library/Containers/.../Data/Library/Application Support/Databases/re-v5rc-26-4025.db
VEXVIA_SKILLS_DB_PATH=/Users/youruser/Library/Containers/.../Data/Library/Application Support/Databases/v5rc-hs-skills.db
```

When available, this source is labeled `vex_via_local` in the rankings page, analysis tab, and reports. That gives you autonomous live-ish standings from the local app cache instead of playing scrape-the-front-end for sport.

To enable the browser-rendered public fallback:

```dotenv
ENABLE_BROWSER_FALLBACK=true
```

Install the Playwright runtime package and browser once:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

This browser fallback is intentionally scoped to the official event page for the configured event SKU. It is not a generic crawler.

## Running Tests

```bash
python -m unittest discover -s tests -v
```

## Future Improvements

- optional authenticated remote dashboard mode
- stronger season-long history and charts
- expanded public source adapters with per-platform parsing improvements
- match-window auto-acceleration during active qualification periods
