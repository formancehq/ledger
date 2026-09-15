#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  printf 'usage: %s EXPECTED_IMPORTS ACTUAL_IMPORTS\n' "$0" >&2
  exit 2
fi

if ! cmp "$1" "$2"; then
  printf 'component import set is outside the canonical Go allowlist\n' >&2
  exit 1
fi
