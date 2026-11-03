#!/usr/bin/env bash
# Exercise an adjacent published RC -> exact release-branch HEAD.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$script_dir/../.." && pwd)"
if [ "${LEDGER_UPGRADE_SHELL:-}" != 1 ]; then
	exec nix develop "$repo#upgrade" --command "$script_dir/run_operator.sh" "$@"
fi
[ "$#" -eq 1 ] || { echo "usage: tests/upgrade/run_operator.sh release/v3.0" >&2; exit 2; }
branch="$1"
[ "$branch" = release/v3.0 ] || { echo "invalid release branch: $branch" >&2; exit 2; }
transition="${UPGRADE_TRANSITION:-rc2-to-rc3}"
case "$transition" in
	rc2-to-rc3) source_tag=v3.0.0-rc.2; target_tag=v3.0.0-rc.3; rejected_tag=v3.0.0-rc.1; rollout=rolling ;;
	rc3-to-rc4) source_tag=v3.0.0-rc.3; target_tag=v3.0.0-rc.4; rejected_tag=v3.0.0-rc.2; rollout=stopped ;;
	*) echo "unsupported upgrade transition: $transition" >&2; exit 2 ;;
esac
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
release_head="$(git -C "$repo" rev-parse --verify "refs/remotes/origin/$branch^{commit}")"
target_sha="$(git -C "$repo" rev-parse --verify "${TARGET_REF:-$release_head}^{commit}")"
harness_sha="$(git -C "$repo" rev-parse --verify HEAD)"
[ "$(gh release view "$source_tag" --json isDraft --jq '.isDraft')" = false ] || {
	echo "$source_tag is not a published release" >&2; exit 2;
}
source_sha="$(git -C "$repo" rev-list -n 1 "$source_tag")"
git -C "$repo" merge-base --is-ancestor "$source_sha" "$target_sha" || {
	echo "target $target_sha does not descend from $source_tag" >&2; exit 2;
}
git -C "$repo" merge-base --is-ancestor "$harness_sha" "$target_sha" || {
	echo "target $target_sha does not contain harness $harness_sha; integrate main into $branch first" >&2; exit 2;
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
printf 'branch=%s\ntransition=%s\nrollout=%s\nreleaseHeadSHA=%s\nsourceTag=%s\nsourceSHA=%s\ntargetTag=%s\ntargetSHA=%s\nharnessSHA=%s\n' \
	"$branch" "$transition" "$rollout" "$release_head" "$source_tag" "$source_sha" "$target_tag" "$target_sha" "$harness_sha" | tee "$workdir/revisions.txt"

target_tree="$workdir/target"
source_tree="$workdir/source"
mkdir -p "$source_tree" "$target_tree"
git -C "$repo" archive "$source_sha" | tar -x -C "$source_tree"
git -C "$repo" archive "$target_sha" | tar -x -C "$target_tree"
source_operator_image="${UPGRADE_OPERATOR_IMAGE_REPOSITORY:-ghcr.io/formancehq/ledger-operator}:$source_tag"
operator_image="ledger-operator:$target_tag"
docker build -q --build-arg "OPERATOR_VERSION=$target_tag" -t "$operator_image" "$target_tree/misc/operator" | tee "$workdir/operator-build.txt"
docker pull "$source_image" | tee "$workdir/source-pull.txt"
docker pull "$source_operator_image" | tee "$workdir/source-operator-pull.txt"
source_id="$(docker image inspect "$source_image" --format '{{.Id}}')"
source_operator_id="$(docker image inspect "$source_operator_image" --format '{{.Id}}')"
target_operator_id="$(docker image inspect "$operator_image" --format '{{.Id}}')"
build_tag="ledger-upgrade:build-${target_sha:0:12}"
docker build -q -t "$build_tag" "$target_tree" | tee "$workdir/target-build.txt"
target_id="$(docker image inspect "$build_tag" --format '{{.Id}}')"
target_repository=ledger-upgrade
target_image="$target_repository:$target_tag"
docker tag "$build_tag" "$target_image"
printf 'sourceImage=%s\nsourceImageID=%s\ntargetImage=%s\ntargetImageID=%s\nsourceOperatorImage=%s\nsourceOperatorImageID=%s\ntargetOperatorImage=%s\ntargetOperatorImageID=%s\n' \
	"$source_image" "$source_id" "$target_image" "$target_id" "$source_operator_image" "$source_operator_id" "$operator_image" "$target_operator_id" | tee -a "$workdir/revisions.txt"

kind create cluster --name "$cluster"
kind get kubeconfig --name "$cluster" > "$workdir/kubeconfig"
kind load docker-image "$source_image" "$target_image" "$source_operator_image" "$operator_image" --name "$cluster"
source_chart="$source_tree/misc/operator/helm/operator"
target_chart="$target_tree/misc/operator/helm/operator"
helm dependency build "$source_chart"
helm dependency build "$target_chart"
helm upgrade --install ledger-operator "$source_chart" --namespace ledger-system --create-namespace \
	--kubeconfig "$workdir/kubeconfig" --kube-context "kind-$cluster" \
	--set "image.repository=${source_operator_image%:*}" --set "image.tag=${source_operator_image##*:}" \
	--set image.pullPolicy=Never --set leaderElection=false \
	--set pvcProtection.enabled=false --wait --timeout 5m

UPGRADE_CLUSTER_SCRIPT="$script_dir/cluster_operator_upgrade.sh" \
UPGRADE_KUBECONFIG="$workdir/kubeconfig" UPGRADE_KUBE_CONTEXT="kind-$cluster" \
UPGRADE_TARGET_OPERATOR_CHART="$target_chart" UPGRADE_TARGET_OPERATOR_IMAGE="$operator_image" \
UPGRADE_TARGET_OPERATOR_IMAGE_ID="$target_operator_id" \
UPGRADE_SOURCE_OPERATOR_IMAGE="$source_operator_image" UPGRADE_SOURCE_OPERATOR_IMAGE_ID="$source_operator_id" \
UPGRADE_SOURCE_REPOSITORY="$source_repository" UPGRADE_SOURCE_TAG="$source_tag" \
UPGRADE_TARGET_REPOSITORY="$target_repository" UPGRADE_TARGET_TAG="$target_tag" \
UPGRADE_TARGET_IMAGE_ID="$target_id" \
	UPGRADE_ROLLOUT="$rollout" UPGRADE_REJECTED_TAG="$rejected_tag" \
	UPGRADE_EVIDENCE_DIR="$workdir" \
	chainsaw test --config "$script_dir/chainsaw/config.yaml" \
		--test-dir "$script_dir/operator" --kube-context "kind-$cluster" --skip-delete \
		2>&1 | tee "$workdir/chainsaw.log"
