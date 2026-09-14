#!/usr/bin/env bash
set -euo pipefail

readonly plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly repository_root="$(cd "$plugin_root/../../.." && pwd -P)"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

precommit_plan="$(cd "$repository_root" && just --dry-run pre-commit 2>&1)"
coverage_plan="$(cd "$repository_root" && just --dry-run test-coverage 2>&1)"
plugin_test_plan="$(cd "$plugin_root" && just --dry-run test 2>&1)"

[[ "$precommit_plan" == *'cd plugins/fctl/ledger-v3 && just build-component'* ]] || fail 'root pre-commit omits the Ledger v3 component build check'
[[ "$coverage_plan" == *'cd plugins/fctl/ledger-v3 && just test'* ]] || fail 'root test-coverage omits the Ledger v3 test gate'
[[ "$plugin_test_plan" == *'scripts/check-coverage-threshold.sh'* ]] || fail 'Ledger v3 tests omit the coverage threshold'
[[ "$plugin_test_plan" == *'go test -race -coverpkg=./... -coverprofile='* ]] || fail 'Ledger v3 coverage is not module-wide'
[[ "$plugin_test_plan" == *'-tags=fctl_component_guest -coverprofile='* ]] || fail 'Ledger v3 coverage omits the component-guest entrypoint'

printf 'root Ledger v3 gate reachability: ok\n'
