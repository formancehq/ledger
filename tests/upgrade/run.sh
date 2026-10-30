#!/usr/bin/env bash
# Build both pinned revisions as Linux images, then run the Chainsaw scenario
# in a dedicated kind cluster. No branch is merged or moved by this script.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$script_dir/../.." && pwd)"
if [ "${LEDGER_UPGRADE_SHELL:-}" != 1 ]; then
	exec nix develop "$repo#upgrade" --command "$script_dir/run.sh" "$@"
fi
target_ref="${TARGET_REF:-refs/remotes/origin/release/v3.0}"
source_ref=v3.0.0-rc.1
[ "$#" -eq 0 ] || { echo "usage: tests/upgrade/run.sh" >&2; exit 2; }
[ -z "$(git -C "$repo" status --porcelain)" ] || {
	echo "the harness checkout must be clean so its recorded SHA identifies the test code" >&2
	exit 2
}
if [ "$target_ref" = refs/remotes/origin/release/v3.0 ]; then
	git -C "$repo" fetch origin +refs/heads/release/v3.0:refs/remotes/origin/release/v3.0
fi
source_sha="$(git -C "$repo" rev-parse --verify "$source_ref^{commit}")"
target_sha="$(git -C "$repo" rev-parse --verify "$target_ref^{commit}")"
harness_sha="$(git -C "$repo" rev-parse --verify HEAD)"
git -C "$repo" merge-base --is-ancestor "$harness_sha" "$target_sha" || {
	echo "target $target_sha does not contain harness $harness_sha; wait for main integration" >&2; exit 2;
}
git -C "$repo" merge-base --is-ancestor "$source_sha" "$target_sha" || {
	echo "target $target_sha does not descend from rc.1 $source_sha" >&2; exit 2;
}

workdir="$(mktemp -d "${TMPDIR:-/tmp}/ledger-upgrade.XXXXXX")"
cluster="ledger-upgrade-$$"
cleanup() {
	kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
	if [ -n "${KEEP_WORKDIR:-}" ]; then echo "upgrade evidence: $workdir"; else rm -rf "$workdir"; fi
}
trap cleanup EXIT
printf 'source=%s\ntarget=%s\nharness=%s\n' "$source_sha" "$target_sha" "$harness_sha" | tee "$workdir/revisions.txt"
mkdir -p "$workdir/source" "$workdir/target"
git -C "$repo" archive "$source_sha" | tar -x -C "$workdir/source"
git -C "$repo" archive "$target_sha" | tar -x -C "$workdir/target"

case "$(docker info --format '{{.Architecture}}')" in
	aarch64|arm64) goarch=arm64 ;;
	x86_64|amd64) goarch=amd64 ;;
	*) echo "unsupported Docker architecture" >&2; exit 2 ;;
esac
build_revision() {
	local name="$1" tree="$2" context="$workdir/image-$1" image="ledger-upgrade:$1-${3:0:12}"
	mkdir -p "$context/tmp" || return 1
	chmod 1777 "$context/tmp" || return 1
	touch "$context/tmp/.keep" || return 1
	if [ -n "${IN_NIX_SHELL:-}" ]; then
		(cd "$tree" && CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -tags invariants -o "$context/ledger-server" . && \
			CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" go build -tags invariants -o "$context/ledgerctl" ./cmd/ledgerctl) || return 1
	else
		(cd "$tree" && nix develop .#ci --command bash -c \
			'CGO_ENABLED=0 GOOS=linux GOARCH="$1" go build -tags invariants -o "$2/ledger-server" . && CGO_ENABLED=0 GOOS=linux GOARCH="$1" go build -tags invariants -o "$2/ledgerctl" ./cmd/ledgerctl' \
			bash "$goarch" "$context") || return 1
	fi
	shasum -a 256 "$context/ledger-server" "$context/ledgerctl" | tee "$workdir/$name-binaries.sha256" >&2 || return 1
	docker build -q -f "$script_dir/Dockerfile" -t "$image" "$context" > "$workdir/$name-docker-build.txt" || return 1
	docker image inspect "$image" --format '{{.Id}}' | tee "$workdir/$name-image-id.txt" >&2 || return 1
	printf '%s\n' "$image"
}
source_image="$(build_revision source "$workdir/source" "$source_sha")" || exit 1
target_image="$(build_revision target "$workdir/target" "$target_sha")" || exit 1
printf 'sourceImage=%s\ntargetImage=%s\n' "$source_image" "$target_image" | tee -a "$workdir/revisions.txt"

kind create cluster --name "$cluster"
kind load docker-image "$source_image" "$target_image" --name "$cluster"
kind get kubeconfig --name "$cluster" > "$workdir/kubeconfig"
UPGRADE_CLUSTER_SCRIPT="$script_dir/cluster_upgrade.sh" \
UPGRADE_KUBECONFIG="$workdir/kubeconfig" UPGRADE_KUBE_CONTEXT="kind-$cluster" \
UPGRADE_SOURCE_IMAGE="$source_image" UPGRADE_TARGET_IMAGE="$target_image" \
UPGRADE_EVIDENCE_DIR="$workdir" \
	chainsaw test --config "$script_dir/chainsaw/config.yaml" \
		--test-dir "$script_dir/chainsaw" --kube-context "kind-$cluster" \
		2>&1 | tee "$workdir/chainsaw.log"
