#!/bin/zsh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing virtualenv python at ${PYTHON_BIN}" >&2
  exit 1
fi

cd "${PROJECT_DIR}"
"${PYTHON_BIN}" main.py --publish-static
