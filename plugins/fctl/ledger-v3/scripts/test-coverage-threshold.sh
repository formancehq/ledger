#!/usr/bin/env bash
set -euo pipefail

readonly script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly checker="$script_root/check-coverage-threshold.sh"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

expect_failure() {
  local expected="$1"
  shift
  local output
  if output="$($checker "$@" 2>&1)"; then
    fail "coverage check unexpectedly succeeded: $*"
  fi
  [[ "$output" == *"$expected"* ]] || fail "missing diagnostic '$expected' in: $output"
}

[[ -x "$checker" ]] || fail "coverage checker is missing or not executable: $checker"

[[ "$($checker 80.0 80)" == 'coverage 80.0% meets minimum 80.0%' ]] || fail 'threshold equality was rejected'
[[ "$($checker 89.1 80)" == 'coverage 89.1% meets minimum 80.0%' ]] || fail 'coverage above threshold was rejected'
expect_failure 'coverage 79.9% is below minimum 80.0%' 79.9 80
expect_failure 'coverage percentage must be numeric' nope 80
expect_failure 'minimum coverage must be between 0 and 100' 90 101

printf 'coverage threshold contract: ok\n'
