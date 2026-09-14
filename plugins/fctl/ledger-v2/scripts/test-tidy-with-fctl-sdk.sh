#!/usr/bin/env bash
set -euo pipefail

readonly plugin_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly tidy_script="$plugin_root/scripts/tidy-with-fctl-sdk.sh"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
fixture="$test_root/product"
sdk_root="$test_root/sdk-source"
fake_bin="$test_root/bin"
mkdir -p "$fixture/scripts" "$fixture/dep" "$fixture/stale" "$sdk_root/pkg/plugin/sdkfixture" "$fake_bin"
fixture="$(cd "$fixture" && pwd -P)"
sdk_root="$(cd "$sdk_root" && pwd -P)"

[[ -x "$tidy_script" ]] || fail "tidy wrapper is missing or not executable: $tidy_script"
cp "$tidy_script" "$fixture/scripts/tidy-with-fctl-sdk.sh"
cp "$plugin_root/fctl-sdk.lock.json" "$fixture/fctl-sdk.lock.json"
cp "$plugin_root/scripts/read-fctl-sdk-lock.go" "$fixture/scripts/read-fctl-sdk-lock.go"

printf 'module github.com/formancehq/fctl-v2-poc/pkg/plugin\n\ngo 1.25.0\n' >"$sdk_root/pkg/plugin/go.mod"
cat >"$sdk_root/pkg/plugin/sdkfixture/value.go" <<'EOF'
package sdkfixture

const Value = "sdk"
EOF
printf 'module example.test/unused\n\ngo 1.25.0\n' >"$fixture/dep/go.mod"
cat >"$fixture/dep/value.go" <<'EOF'
package unused

const Value = "relative"
EOF
printf 'module example.test/stale\n\ngo 1.25.0\n' >"$fixture/stale/go.mod"
cat >"$fixture/main.go" <<'EOF'
package product

import (
	"example.test/unused"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdkfixture"
)

var Value = sdkfixture.Value + unused.Value
EOF

write_drifted_module() {
  cat >"$fixture/go.mod" <<'EOF'
module example.test/product

go 1.25.0

require (
	example.test/unused v0.0.0
	example.test/stale v0.0.0
	github.com/formancehq/fctl-v2-poc/pkg/plugin v0.0.0
)

replace example.test/unused => ./dep

replace example.test/stale => ./stale
EOF
  : >"$fixture/go.sum"
}

assert_no_temporary_files() {
  if find "$fixture" -maxdepth 1 -name '.fctl-sdk-tidy.*' -print -quit | grep . >/dev/null; then
    find "$fixture" -maxdepth 1 -name '.fctl-sdk-tidy.*' -print >&2
    fail 'tidy wrapper leaked temporary files'
  fi
}

write_drifted_module
before_mod="$(shasum -a 256 "$fixture/go.mod" | awk '{print $1}')"
before_sum="$(shasum -a 256 "$fixture/go.sum" | awk '{print $1}')"
if FCTL_SDK_ROOT="$sdk_root" "$fixture/scripts/tidy-with-fctl-sdk.sh" --check >"$test_root/check.stdout" 2>"$test_root/check.stderr"; then
  fail 'tidy --check accepted module drift'
fi
grep -F 'plugin module metadata is not tidy' "$test_root/check.stderr" >/dev/null || fail 'tidy --check omitted its drift diagnostic'
[[ "$(shasum -a 256 "$fixture/go.mod" | awk '{print $1}')" == "$before_mod" ]] || fail 'tidy --check mutated go.mod'
[[ "$(shasum -a 256 "$fixture/go.sum" | awk '{print $1}')" == "$before_sum" ]] || fail 'tidy --check mutated go.sum'
assert_no_temporary_files

FCTL_SDK_ROOT="$sdk_root" "$fixture/scripts/tidy-with-fctl-sdk.sh"
rg -n 'example.test/stale v0.0.0' "$fixture/go.mod" && fail 'mutating tidy retained an unused requirement'
grep -F 'example.test/unused v0.0.0' "$fixture/go.mod" >/dev/null || fail 'mutating tidy lost an imported relative requirement'
rg -n 'replace github.com/formancehq/fctl-v2-poc/pkg/plugin' "$fixture/go.mod" && fail 'mutating tidy persisted the SDK replacement'
grep -F 'replace example.test/unused => ./dep' "$fixture/go.mod" >/dev/null || fail 'mutating tidy lost an unrelated relative replacement'
FCTL_SDK_ROOT="$sdk_root" "$fixture/scripts/tidy-with-fctl-sdk.sh" --check
assert_no_temporary_files

before_mod="$(shasum -a 256 "$fixture/go.mod" | awk '{print $1}')"
before_sum="$(shasum -a 256 "$fixture/go.sum" | awk '{print $1}')"
cat >"$fixture/main.go" <<'EOF'
package product

import "github.com/formancehq/fctl-v2-poc/pkg/plugin/missing"

var Value = missing.Value
EOF
if FCTL_SDK_ROOT="$sdk_root" "$fixture/scripts/tidy-with-fctl-sdk.sh" >"$test_root/failure.stdout" 2>"$test_root/failure.stderr"; then
  fail 'tidy unexpectedly accepted an invalid SDK module'
fi
[[ "$(shasum -a 256 "$fixture/go.mod" | awk '{print $1}')" == "$before_mod" ]] || fail 'failed tidy mutated go.mod'
[[ "$(shasum -a 256 "$fixture/go.sum" | awk '{print $1}')" == "$before_sum" ]] || fail 'failed tidy mutated go.sum'
assert_no_temporary_files

real_go="$(command -v go)"
cat >"$fake_bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == 'mod' && "$2" == 'tidy' ]]; then
  for argument in "$@"; do
    if [[ "$argument" == -modfile=* ]]; then
      printf '%s\n' "${argument#-modfile=}" >"$CAPTURE_MODFILE"
    fi
  done
  kill -TERM "$PPID"
  exit 99
fi
exec "$REAL_GO" "$@"
EOF
chmod +x "$fake_bin/go"
capture_modfile="$test_root/signalled-modfile"
if PATH="$fake_bin:$PATH" REAL_GO="$real_go" CAPTURE_MODFILE="$capture_modfile" \
  FCTL_SDK_ROOT="$sdk_root" "$fixture/scripts/tidy-with-fctl-sdk.sh"; then
  fail 'tidy wrapper survived SIGTERM during go mod tidy'
fi
signalled_modfile="$(cat "$capture_modfile")"
[[ "$(dirname "$signalled_modfile")" == "$fixture" ]] || fail "temporary modfile was not created beside the product go.mod"
[[ ! -e "$signalled_modfile" ]] || fail "SIGTERM left temporary modfile behind: $signalled_modfile"
assert_no_temporary_files

if rg -n '/Users/|/home/|[A-Za-z]:\\' "$plugin_root/go.mod" "$plugin_root/fctl-sdk.lock.json" "$plugin_root/scripts/tidy-with-fctl-sdk.sh"; then
  fail 'tracked tidy contract contains a workstation-absolute path'
fi

printf 'fctl SDK tidy contract: ok\n'
