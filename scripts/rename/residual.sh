#!/usr/bin/env bash
# Usage: scripts/rename/residual.sh '<extended regex>'
# Prints tracked lines that still match, minus the fixed exclusions.
set -uo pipefail
git grep -n -I -E "$1" -- . \
  ':!docs/superpowers' ':!CHANGELOG*' ':!scripts/rename' ':!*package-lock.json' ':!*pnpm-lock.yaml' \
  ':!*SecurityConfigurationValidator*' ':!*ConsumerGroupBlacklistFilter*' \
  ':!*V1__baseline_schema.sql'
rc=$?
# git grep: 0 = matches, 1 = no matches, anything else = error (e.g. invalid regex).
if [ "$rc" -gt 1 ]; then
  echo "residual.sh: git grep failed with exit code $rc" >&2
  exit "$rc"
fi
exit 0
