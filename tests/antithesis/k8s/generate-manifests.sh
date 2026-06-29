#!/usr/bin/env bash
# Assembles Kubernetes manifests for the Antithesis config image.
#
# Usage: ./generate-manifests.sh [TAG]
#   TAG defaults to "antithesis" and replaces __TAG__ placeholders in manifests.
#
# The script renders the operator Helm chart, copies CRDs, and assembles
# all manifests into a manifests/ directory ready for the config image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TAG="${1:-antithesis}"
OUT="$SCRIPT_DIR/manifests"

ANTITHESIS_REGISTRY="${ANTITHESIS_REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/formance-repository}"
LEDGER_IMAGE_NAME="${LEDGER_IMAGE_NAME:-ledger}"

rm -rf "$OUT"
mkdir -p "$OUT"

echo "==> Generating Antithesis k8s manifests (tag=$TAG)"

# 1. CRDs — copy from operator config
for f in "$REPO_ROOT/misc/operator/config/crd/bases/"*.yaml; do
  cp "$f" "$OUT/$(basename "$f")"
done

# 2. Operator — render Helm chart then strip Helm hook resources.
#    kapp (used by Antithesis) does not understand helm.sh/hook annotations,
#    so pre-delete jobs and their RBAC would be applied at startup and fail.
helm dependency update "$REPO_ROOT/misc/operator/helm/operator" --skip-refresh 2>/dev/null || true
helm template ledger-operator "$REPO_ROOT/misc/operator/helm/operator" \
  --set ledger-operator-crds.create=false \
  --set image.repository="$ANTITHESIS_REGISTRY/ledger-operator" \
  --set image.tag="$TAG" \
  --set image.pullPolicy=IfNotPresent \
  --set ledgerImage.registry="$ANTITHESIS_REGISTRY" \
  --set ledgerImage.name="$LEDGER_IMAGE_NAME" \
  --set ledgerImage.tag="$TAG" \
  --set leaderElection=false \
  --set "resources.requests.cpu=50m" \
  --set "resources.requests.memory=64Mi" \
  --set "resources.limits.memory=128Mi" \
  | awk '
    /^---/ { if (buf != "" && !skip) print buf; buf="---\n"; skip=0; next }
    /helm\.sh\/hook/ { skip=1 }
    { buf = buf $0 "\n" }
    END { if (buf != "" && !skip) print buf }
  ' > "$OUT/operator.yaml"

# 3. Static manifests — substitute tag, registry, image name
for tmpl in kapp-config.yaml minio.yaml nats.yaml ledgerservice.yaml workload.yaml; do
  sed "s|__TAG__|$TAG|g; s|__REGISTRY__|$ANTITHESIS_REGISTRY|g; s|__IMAGE_NAME__|$LEDGER_IMAGE_NAME|g" \
    "$SCRIPT_DIR/$tmpl" > "$OUT/$tmpl"
done

echo "==> Manifests written to $OUT/"
ls -1 "$OUT/"
