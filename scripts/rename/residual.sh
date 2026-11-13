#!/usr/bin/env bash
# Usage: scripts/rename/residual.sh '<extended regex>'
# Prints tracked lines that still match, minus the fixed exclusions.
set -euo pipefail
git grep -n -I -E "$1" -- . \
  ':!docs/superpowers' ':!CHANGELOG*' ':!scripts/rename' ':!*lock*' \
  ':!*SecurityConfigurationValidator*' ':!*ConsumerGroupBlacklistFilter*' \
  ':!*V1__baseline_schema.sql' || true
