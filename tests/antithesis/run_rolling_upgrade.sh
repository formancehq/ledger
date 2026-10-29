#!/usr/bin/env bash
# Upgrade the published rc.1 binary to the exact release/v2.0 head fetched for
# this run. The two source trees and both binaries are pinned before boot.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$script_dir/../.." && pwd)"
source_ref="v3.0.0-rc.1"
target_ref="${TARGET_REF:-refs/remotes/origin/release/v2.0}"
[ "$#" -eq 0 ] || { echo "usage: run_rolling_upgrade.sh (set UPGRADE_PHASE_SECONDS to tune the mixed window)" >&2; exit 2; }
[ -z "$(git -C "$repo" status --porcelain)" ] || {
	echo "the harness checkout must be clean so its recorded SHA identifies the test code" >&2
	exit 2
}

# Refresh only the target branch. No local branch is moved or merged. An
# explicit TARGET_REF (SHA or other local ref) is for rehearsals.
if [ "$target_ref" = refs/remotes/origin/release/v2.0 ]; then
	git -C "$repo" fetch origin +refs/heads/release/v2.0:refs/remotes/origin/release/v2.0
fi
source_sha="$(git -C "$repo" rev-parse --verify "$source_ref^{commit}")"
target_sha="$(git -C "$repo" rev-parse --verify "$target_ref^{commit}")"
harness_sha="$(git -C "$repo" rev-parse --verify HEAD)"
git -C "$repo" merge-base --is-ancestor "$harness_sha" "$target_sha" || {
	echo "target $target_sha does not contain this harness commit $harness_sha; wait for main integration" >&2
	exit 2
}
git -C "$repo" merge-base --is-ancestor "$source_sha" "$target_sha" || {
	echo "target $target_sha does not descend from source $source_sha" >&2
	exit 2
}

workdir="$(mktemp -d "${TMPDIR:-/tmp}/ledger-rolling-upgrade.XXXXXX")"
cleanup() {
	if [ -n "${KEEP_WORKDIR:-}" ]; then echo "upgrade build work dir kept at $workdir"; else rm -rf "$workdir"; fi
}
trap cleanup EXIT
mkdir -p "$workdir/source" "$workdir/target"
git -C "$repo" archive "$source_sha" | tar -x -C "$workdir/source"
git -C "$repo" archive "$target_sha" | tar -x -C "$workdir/target"
printf 'source ref=%s sha=%s\ntarget ref=%s sha=%s\nharness sha=%s\n' \
	"$source_ref" "$source_sha" "$target_ref" "$target_sha" "$harness_sha" | tee "$workdir/revisions.txt"

source_bin="$workdir/ledger-rc1"
echo "building source rc.1 binary from $source_sha"
if [ -n "${IN_NIX_SHELL:-}" ]; then
	(cd "$workdir/source" && go build -tags invariants -o "$source_bin" .)
else
	(cd "$workdir/source" && nix develop .#ci --command go build -tags invariants -o "$source_bin" .)
fi
shasum -a 256 "$source_bin" | tee "$workdir/source-binary.sha256"

echo "running three-node rolling upgrade to $target_sha"
UPGRADE_SOURCE_BIN="$source_bin" REPO="$workdir/target" MODEL_HARNESS_REPO="$workdir/target" \
	"$script_dir/run_model_test.sh" --cluster
