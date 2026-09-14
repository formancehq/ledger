#!/usr/bin/env bash
set -euo pipefail

readonly plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly lock_path="$plugin_root/fctl-sdk.lock.json"
readonly lock_reader="$plugin_root/scripts/read-fctl-sdk-lock.go"

mode='apply'
if [[ "$#" -gt 1 ]]; then
  printf 'usage: tidy-with-fctl-sdk.sh [--check]\n' >&2
  exit 2
fi
if [[ "$#" -eq 1 ]]; then
  [[ "$1" == '--check' ]] || { printf 'usage: tidy-with-fctl-sdk.sh [--check]\n' >&2; exit 2; }
  mode='check'
fi
readonly mode

[[ -n "${FCTL_SDK_ROOT:-}" ]] || { printf 'FCTL_SDK_ROOT is required\n' >&2; exit 2; }
[[ -d "$FCTL_SDK_ROOT" ]] || { printf 'FCTL_SDK_ROOT is not a directory: %s\n' "$FCTL_SDK_ROOT" >&2; exit 2; }

IFS=$'\t' read -r module_path _ _ sdk_path _ _ _ < <(
  GOWORK=off go run "$lock_reader" "$lock_path"
)
readonly module_path sdk_path
sdk_directory="$(cd "$FCTL_SDK_ROOT/$sdk_path" && pwd -P)"
readonly sdk_directory

temporary_marker="$(mktemp "$plugin_root/.fctl-sdk-tidy.XXXXXXXX")"
temporary_mod="$temporary_marker.mod"
temporary_sum="$temporary_marker.sum"
readonly temporary_marker temporary_mod temporary_sum
trap 'rm -f -- "$temporary_marker" "$temporary_mod" "$temporary_sum"' EXIT
mv "$temporary_marker" "$temporary_mod"
cp "$plugin_root/go.mod" "$temporary_mod"
original_sum_exists='false'
if [[ -f "$plugin_root/go.sum" ]]; then
  cp "$plugin_root/go.sum" "$temporary_sum"
  original_sum_exists='true'
else
  : >"$temporary_sum"
fi
readonly original_sum_exists

GOWORK=off go mod edit -modfile="$temporary_mod" -replace "$module_path=$sdk_directory"
(
  cd "$plugin_root"
  GOWORK=off go mod tidy -modfile="$temporary_mod"
)
GOWORK=off go mod edit -modfile="$temporary_mod" -dropreplace "$module_path"

sum_differs='false'
if [[ "$original_sum_exists" == 'true' ]]; then
  cmp -s "$plugin_root/go.sum" "$temporary_sum" || sum_differs='true'
elif [[ -s "$temporary_sum" ]]; then
  sum_differs='true'
fi
readonly sum_differs

if [[ "$mode" == 'check' ]]; then
  if ! cmp -s "$plugin_root/go.mod" "$temporary_mod" || [[ "$sum_differs" == 'true' ]]; then
    printf 'plugin module metadata is not tidy\n' >&2
    diff -u "$plugin_root/go.mod" "$temporary_mod" >&2 || true
    if [[ "$original_sum_exists" == 'true' ]]; then
      diff -u "$plugin_root/go.sum" "$temporary_sum" >&2 || true
    elif [[ -s "$temporary_sum" ]]; then
      diff -u /dev/null "$temporary_sum" >&2 || true
    fi
    exit 1
  fi
  exit 0
fi

cp "$temporary_mod" "$plugin_root/go.mod"
if [[ "$original_sum_exists" == 'true' || -s "$temporary_sum" ]]; then
  cp "$temporary_sum" "$plugin_root/go.sum"
fi
