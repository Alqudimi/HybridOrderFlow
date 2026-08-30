#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m compileall -q .
pytest -v
python3 scripts/run_full_verification.py
python3 scripts/generate_report.py
echo "All verification checks completed successfully!"
