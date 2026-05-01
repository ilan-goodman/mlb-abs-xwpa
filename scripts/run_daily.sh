#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."
python3 scripts/update_abs_xwpa.py --year 2026 --model-scope season
