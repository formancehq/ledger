#!/usr/bin/env bash
set -euo pipefail

[[ "$#" -eq 2 ]] || {
  printf 'usage: check-coverage-threshold.sh ACTUAL MINIMUM\n' >&2
  exit 2
}

readonly actual="$1"
readonly minimum="$2"
readonly numeric_pattern='^([0-9]+([.][0-9]+)?|[.][0-9]+)$'

[[ "$actual" =~ $numeric_pattern ]] || {
  printf 'coverage percentage must be numeric: %s\n' "$actual" >&2
  exit 2
}
[[ "$minimum" =~ $numeric_pattern ]] || {
  printf 'minimum coverage must be numeric: %s\n' "$minimum" >&2
  exit 2
}

awk -v actual="$actual" -v minimum="$minimum" 'BEGIN {
  if (minimum < 0 || minimum > 100) {
    printf "minimum coverage must be between 0 and 100: %.1f%%\n", minimum > "/dev/stderr"
    exit 2
  }
  if (actual < 0 || actual > 100) {
    printf "coverage percentage must be between 0 and 100: %.1f%%\n", actual > "/dev/stderr"
    exit 2
  }
  if (actual < minimum) {
    printf "coverage %.1f%% is below minimum %.1f%%\n", actual, minimum > "/dev/stderr"
    exit 1
  }
  printf "coverage %.1f%% meets minimum %.1f%%\n", actual, minimum
}'
