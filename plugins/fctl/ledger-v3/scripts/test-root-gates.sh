#!/usr/bin/env bash
set -euo pipefail

readonly plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly repository_root="$(cd "$plugin_root/../../.." && pwd -P)"
readonly lock_path="$plugin_root/fctl-sdk.lock.json"
readonly lock_reader="$plugin_root/scripts/read-fctl-sdk-lock.go"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

precommit_plan="$(cd "$repository_root" && just --dry-run pre-commit 2>&1)"
coverage_plan="$(cd "$repository_root" && just --dry-run test-coverage 2>&1)"
component_plan="$(cd "$repository_root" && just --dry-run fctl-ledger-v3-build-component 2>&1)"
plugin_test_plan="$(cd "$plugin_root" && just --dry-run test 2>&1)"

[[ "$precommit_plan" == *'cd plugins/fctl/ledger-v3 && just test'* ]] || fail 'root pre-commit omits the Ledger v3 test gate'
# The component build stays reachable from the root, but as its own heavier
# gate: it is the only path needing the Rust authoring toolchain, and pulling
# that into every pre-commit run makes unrelated Go jobs fetch crates.
[[ "$component_plan" == *'cd plugins/fctl/ledger-v3 && just build-component'* ]] || fail 'root component gate omits the Ledger v3 component build'
[[ "$component_plan" == *'nix shell .#componentize-go .#wasi-virt .#wasm-tools .#wasm-opt'* ]] || fail 'Ledger v3 component build does not isolate the authoring toolchain'
[[ "$coverage_plan" == *'cd plugins/fctl/ledger-v3 && just test'* ]] || fail 'root test-coverage omits the Ledger v3 test gate'
[[ "$plugin_test_plan" == *'scripts/check-coverage-threshold.sh'* ]] || fail 'Ledger v3 tests omit the coverage threshold'
[[ "$plugin_test_plan" == *'go test -race -coverpkg=./... -coverprofile='* ]] || fail 'Ledger v3 coverage is not module-wide'
[[ "$plugin_test_plan" == *'-tags=fctl_component_guest -coverprofile='* ]] || fail 'Ledger v3 coverage omits the component-guest entrypoint'

IFS=$'\t' read -r _ _ _ sdk_path expected_nar_hash wit_path expected_wit_hash < <(
  GOWORK=off go run "$lock_reader" "$lock_path"
)
system="$(nix eval --impure --raw --expr builtins.currentSystem)"
sdk_root="$(nix eval --raw "$repository_root#devShells.$system.default.FCTL_SDK_ROOT")"
[[ -d "$sdk_root" ]] || fail "root development shell SDK source is missing: $sdk_root"
actual_nar_hash="$(nix --extra-experimental-features nix-command hash path --type sha256 --sri "$sdk_root/$sdk_path")"
[[ "$actual_nar_hash" == "$expected_nar_hash" ]] || fail 'root development shell SDK source does not match the content lock'
actual_wit_hash="$(shasum -a 256 "$sdk_root/$wit_path" | awk '{print $1}')"
[[ "$actual_wit_hash" == "$expected_wit_hash" ]] || fail 'root development shell SDK WIT does not match the content lock'

printf 'root Ledger v3 gate reachability: ok\n'
