#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./download_vexworlds.sh <vexworlds-url-or-broadcast-id> [quality] [output-name]

Examples:
  ./download_vexworlds.sh \
    'https://www.vexworlds.tv/#/viewer/broadcasts/practice--qualification-matches-technology-mv6olnh0lcdsjnediguv/xponhawezq7adhmfdycu'

  ./download_vexworlds.sh xponhawezq7adhmfdycu 1080p60

Qualities:
  1080p60  720p60  480p60  240p60  best

Ubuntu deps:
  sudo apt-get update
  sudo apt-get install -y curl python3 ffmpeg
  python3 -m pip install -U yt-dlp

Notes:
  - Downloads use BoxCast's public API exposed by the VEX Worlds player.
  - Output is resumable if the temporary .part/.ytdl files remain in place.
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[-] Missing dependency: $cmd" >&2
    exit 1
  fi
}

extract_broadcast_id() {
  local input="$1"
  if [[ "$input" =~ ^https?:// ]]; then
    python3 - "$input" <<'PY'
import re
import sys

url = sys.argv[1]
m = re.search(r'/viewer/broadcasts/[^/]+/([A-Za-z0-9_-]+)', url)
if not m:
    m = re.search(r'/broadcasts/([A-Za-z0-9_-]+)', url)
if not m:
    raise SystemExit("[-] Could not extract broadcast id from URL")
print(m.group(1))
PY
  else
    printf '%s\n' "$input"
  fi
}

pick_variant_url() {
  local master_url="$1"
  local quality="$2"

  if [[ "$quality" == "best" ]]; then
    printf '%s\n' "$master_url"
    return 0
  fi

  python3 - "$master_url" "$quality" <<'PY'
import sys
import urllib.request

master_url = sys.argv[1]
quality = sys.argv[2]
target = {
    "240p60": "/240p.m3u8",
    "480p60": "/480p.m3u8",
    "720p60": "/720p.m3u8",
    "1080p60": "/1080p.m3u8",
}.get(quality)

if not target:
    raise SystemExit(f"[-] Unsupported quality: {quality}")

with urllib.request.urlopen(master_url) as response:
    body = response.read().decode("utf-8", "replace")

lines = [line.strip() for line in body.splitlines() if line.strip()]

for line in lines:
    if line.startswith("https://") and target in line:
        print(line)
        raise SystemExit(0)

raise SystemExit(f"[-] Could not find variant URL for {quality}")
PY
}

main() {
  if [[ $# -lt 1 ]] || [[ "${1:-}" == "-h" ]] || [[ "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_cmd curl
  require_cmd python3
  require_cmd yt-dlp

  local input="$1"
  local quality="${2:-1080p60}"
  local output_name="${3:-}"
  local broadcast_id
  local api_json
  local metadata_json
  local title
  local description
  local master_playlist
  local variant_url
  local safe_title

  broadcast_id="$(extract_broadcast_id "$input")"
  echo "[*] Broadcast ID: $broadcast_id"

  api_json="$(curl -fsSL "https://api.boxcast.com/broadcasts/${broadcast_id}/view")"
  metadata_json="$(curl -fsSL "https://api.boxcast.com/broadcasts/${broadcast_id}")"

  master_playlist="$(
    python3 - <<'PY' "$api_json"
import json
import sys

data = json.loads(sys.argv[1])
if data.get("status") != "recorded":
    raise SystemExit(f"[-] Broadcast status is {data.get('status')!r}, not 'recorded'")
playlist = data.get("playlist")
if not playlist:
    raise SystemExit("[-] BoxCast view API did not return a playlist URL")
print(playlist)
PY
  )"

  variant_url="$(pick_variant_url "$master_playlist" "$quality")"

  title="$(
    python3 - <<'PY' "$metadata_json"
import json
import re
import sys

data = json.loads(sys.argv[1])
title = data.get("name") or data.get("id") or "vexworlds"
title = re.sub(r'[\\/:*?"<>|]+', "_", title).strip()
print(title)
PY
  )"

  description="$(
    python3 - <<'PY' "$metadata_json"
import json
import re
import sys

data = json.loads(sys.argv[1])
desc = data.get("description") or ""
desc = re.sub(r'\s+', " ", desc).strip()
desc = re.sub(r'[\\/:*?"<>|]+', "_", desc)
print(desc)
PY
  )"

  safe_title="${title} [${broadcast_id}] [${quality}]"
  if [[ -n "$description" ]]; then
    safe_title="${safe_title} ${description}"
  fi
  if [[ -n "$output_name" ]]; then
    safe_title="$output_name"
  fi

  echo "[*] Title: $title"
  echo "[*] Quality: $quality"
  echo "[*] Variant: $variant_url"
  echo "[*] Output: ${safe_title}.%(ext)s"

  yt-dlp \
    --continue \
    --fragment-retries 10 \
    --retry-sleep 5 \
    -N 8 \
    -o "${safe_title}.%(ext)s" \
    "$variant_url"
}

main "$@"
