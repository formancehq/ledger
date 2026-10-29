#!/usr/bin/env bash
# Exercise a published release -> exact release-branch HEAD through the operator.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$script_dir/../.." && pwd)"
if [ -z "${IN_NIX_SHELL:-}" ]; then
	exec nix develop "$repo#upgrade" --command "$script_dir/run_operator.sh" "$@"
fi
[ "$#" -eq 1 ] || { echo "usage: tests/upgrade/run_operator.sh release/vX.Y" >&2; exit 2; }
branch="$1"
[[ "$branch" =~ ^release/v[0-9]+\.[0-9]+$ ]] || { echo "invalid release branch: $branch" >&2; exit 2; }
[ -z "$(git -C "$repo" status --porcelain)" ] || {
	echo "the harness checkout must be clean so its SHA identifies the test code" >&2
	exit 2
}

line="${branch#release/v}"
# Fetch only this release line's tags; mutable automation tags such as nightly
# must not collide with an existing local tag in a contributor checkout.
git -C "$repo" fetch --no-tags origin \
	"+refs/heads/$branch:refs/remotes/origin/$branch" \
	"refs/tags/v$line.*:refs/tags/v$line.*"
target_sha="$(git -C "$repo" rev-parse --verify "refs/remotes/origin/$branch^{commit}")"
harness_sha="$(git -C "$repo" rev-parse --verify HEAD)"
source_tag=""
while IFS= read -r candidate; do
	if [ "$(gh release view "$candidate" --json isDraft --jq '.isDraft' 2>/dev/null)" = false ]; then
		source_tag="$candidate"
		break
	fi
done < <(git -C "$repo" tag --merged "$target_sha" | python3 "$script_dir/select_release.py" "$branch")
[ -n "$source_tag" ] || { echo "no published release tag on $branch at $target_sha" >&2; exit 2; }
source_sha="$(git -C "$repo" rev-list -n 1 "$source_tag")"
[ "$source_sha" != "$target_sha" ] || {
	echo "$branch HEAD is already the latest published release $source_tag; nothing to upgrade" >&2
	exit 2
}

mkdir -p "$repo/build/upgrade-operator"
workdir="$(mktemp -d "$repo/build/upgrade-operator/run.XXXXXX")"
cluster="ledger-operator-upgrade-$$"
on_exit() {
	local result="$?" namespace
	if [ -s "$workdir/kubeconfig" ]; then
		kubectl --kubeconfig "$workdir/kubeconfig" get clusters,statefulsets,pods,pvc -A -o yaml > "$workdir/kubernetes.yaml" 2>&1 || true
		kubectl --kubeconfig "$workdir/kubeconfig" -n ledger-system logs deployment/ledger-operator > "$workdir/operator.log" 2>&1 || true
		for namespace in $(kubectl --kubeconfig "$workdir/kubeconfig" get namespaces -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
			case "$namespace" in chainsaw-*)
				kubectl --kubeconfig "$workdir/kubeconfig" -n "$namespace" get pods -o name 2>/dev/null | while IFS= read -r pod; do
					kubectl --kubeconfig "$workdir/kubeconfig" -n "$namespace" logs "$pod" -c ledger > "$workdir/${namespace}-${pod#pod/}.log" 2>&1 || true
				 done ;;
			esac
		done
	fi
	kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
	echo "operator upgrade evidence: $workdir"
	exit "$result"
}
trap on_exit EXIT

source_repository="${UPGRADE_IMAGE_REPOSITORY:-ghcr.io/formancehq/ledger}"
source_image="$source_repository:$source_tag"
printf 'branch=%s\nsourceTag=%s\nsourceSHA=%s\ntargetSHA=%s\nharnessSHA=%s\n' \
	"$branch" "$source_tag" "$source_sha" "$target_sha" "$harness_sha" | tee "$workdir/revisions.txt"

target_tree="$workdir/target"
mkdir -p "$target_tree"
git -C "$repo" archive "$target_sha" | tar -x -C "$target_tree"
operator_image="ledger-operator:head-${target_sha:0:12}"
docker build -q -t "$operator_image" "$target_tree/misc/operator" | tee "$workdir/operator-build.txt"
docker pull "$source_image" | tee "$workdir/source-pull.txt"
source_id="$(docker image inspect "$source_image" --format '{{.Id}}')"
build_tag="ledger-upgrade:build-${target_sha:0:12}"
docker build -q -t "$build_tag" "$target_tree" | tee "$workdir/target-build.txt"
target_id="$(docker image inspect "$build_tag" --format '{{.Id}}')"
target_repository=ledger-upgrade
target_tag="sha-${target_id#sha256:}"
target_image="$target_repository:$target_tag"
docker tag "$build_tag" "$target_image"
printf 'sourceImage=%s\nsourceImageID=%s\ntargetImage=%s\ntargetImageID=%s\noperatorImage=%s\n' \
	"$source_image" "$source_id" "$target_image" "$target_id" "$operator_image" | tee -a "$workdir/revisions.txt"

kind create cluster --name "$cluster"
kind get kubeconfig --name "$cluster" > "$workdir/kubeconfig"
kind load docker-image "$source_image" "$target_image" "$operator_image" --name "$cluster"
chart="$target_tree/misc/operator/helm/operator"
helm dependency build "$chart"
helm upgrade --install ledger-operator "$chart" --namespace ledger-system --create-namespace \
	--kubeconfig "$workdir/kubeconfig" --kube-context "kind-$cluster" \
	--set image.repository=ledger-operator --set "image.tag=${operator_image#*:}" \
	--set image.pullPolicy=Never --set leaderElection=false \
	--set pvcProtection.enabled=false --wait --timeout 5m

UPGRADE_CLUSTER_SCRIPT="$script_dir/cluster_operator_upgrade.sh" \
UPGRADE_KUBECONFIG="$workdir/kubeconfig" UPGRADE_KUBE_CONTEXT="kind-$cluster" \
UPGRADE_SOURCE_REPOSITORY="$source_repository" UPGRADE_SOURCE_TAG="$source_tag" \
UPGRADE_TARGET_REPOSITORY="$target_repository" UPGRADE_TARGET_TAG="$target_tag" \
UPGRADE_TARGET_IMAGE_ID="$target_id" \
	chainsaw test --config "$script_dir/chainsaw/config.yaml" \
		--test-dir "$script_dir/operator" --kube-context "kind-$cluster" \
		2>&1 | tee "$workdir/chainsaw.log"
