#!/usr/bin/env bash
# Offline installation boundary checks; uses only the existing Helm/Bash tools.
set -euo pipefail
cd "$(dirname "$0")/../../.."
work=$(mktemp -d)
trap 'rm -rf "$work" helm/operator/charts' EXIT
helm dependency build helm/operator --skip-refresh >/dev/null
vap=admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy
helm lint helm/crds --strict
helm lint helm/operator --strict
helm template ledger-next-operator helm/operator --namespace ledger-next-system \
  --set watchNamespace=ledger-next --set image.tag=rocksdb-test \
  --set ledgerImage.tag=rocksdb-test --api-versions "$vap" > "$work/all.yaml"
fail() { echo "FAIL: $*" >&2; exit 1; }
expect() { grep -Fq -- "$2" "$1" || fail "missing $2 in $1"; }
# These are the Pebble installation's group, volume keys, and fixed policy names.
# None may appear anywhere in the next installation, including dependency CRDs.
if grep -Eq 'ledger\.formance\.com|(^|[^[:alnum:].-])formance\.com/allow-deletion|ledger-volume-protection-(pvc|pv)' "$work/all.yaml"; then
  fail 'next installation references Pebble identities'
fi
expect "$work/all.yaml" 'image: "ghcr.io/formancehq/ledger-operator:rocksdb-test"'
expect "$work/all.yaml" 'name: LEDGER_IMAGE_TAG'
expect "$work/all.yaml" 'value: "rocksdb-test"'
# Check resource identities and bindings independently of chart labels.
for template in deployment clusterrole clusterrolebinding serviceaccount pre-delete-job validatingadmissionpolicy; do
  helm template ledger-next-operator helm/operator --namespace ledger-next-system \
    --set watchNamespace=ledger-next --api-versions "$vap" \
    --show-only "templates/$template.yaml" > "$work/$template.yaml"
done
for template in deployment clusterrole clusterrolebinding serviceaccount; do
  grep -Fxq '  name: ledger-next-operator' "$work/$template.yaml" || fail "$template identity is not ledger-next-operator"
done
expect "$work/deployment.yaml" '--watch-namespace=ledger-next'
expect "$work/deployment.yaml" 'app.kubernetes.io/name: ledger-next-operator'
expect "$work/clusterrole.yaml" '  - ledger-next.formance.com'
expect "$work/clusterrolebinding.yaml" '  namespace: ledger-next-system'
expect "$work/pre-delete-job.yaml" '  name: ledger-next-operator-pre-delete'
expect "$work/pre-delete-job.yaml" 'apiGroups: ["ledger-next.formance.com"]'
expect "$work/pre-delete-job.yaml" 'kubectl delete credentials.ledger-next.formance.com --all'
expect "$work/pre-delete-job.yaml" '--ignore-not-found --wait --timeout=60s'
if grep -Eq -- '--namespace|--all-namespaces|2>/dev/null|\|\| true' "$work/pre-delete-job.yaml"; then
  fail 'cluster-scoped credential cleanup is namespaced or hides failures'
fi
for volume in pvc pv; do
  expect "$work/validatingadmissionpolicy.yaml" "  name: ledger-next-volume-protection-$volume"
  expect "$work/validatingadmissionpolicy.yaml" "  policyName: ledger-next-volume-protection-$volume"
done
[ "$(grep -c 'ledger-next.formance.com/deletion-protection: enabled' "$work/validatingadmissionpolicy.yaml")" -eq 2 ] || fail 'both bindings must select only next volumes'
expect "$work/validatingadmissionpolicy.yaml" "oldObject.metadata.annotations['ledger-next.formance.com/allow-deletion'] == 'true'"
expect "$work/validatingadmissionpolicy.yaml" 'system:serviceaccount:ledger-next-system:ledger-next-operator'
helm template ledger-next-operator-crds helm/crds > "$work/crds.yaml"
[ "$(grep -c '^kind: CustomResourceDefinition$' "$work/crds.yaml")" -eq 6 ] || fail 'CRD chart must contain exactly six CRDs'
[ "$(grep -c '^  group: ledger-next.formance.com$' "$work/crds.yaml")" -eq 6 ] || fail 'all six CRDs must use the next group'
[ "$(grep -c '^kind: CustomResourceDefinition$' "$work/all.yaml")" -eq 6 ] || fail 'operator dependency must include six CRDs'
for resource in clusters ledgers backups backupruns eventsinks credentials; do
  expect "$work/crds.yaml" "  name: $resource.ledger-next.formance.com"
done
# Credentials cannot be scoped by the operator's namespace/watch namespace.
helm template ledger-next-operator-crds helm/crds \
  --show-only templates/ledger-next.formance.com_credentials.yaml > "$work/credentials.yaml"
expect "$work/credentials.yaml" '  scope: Cluster'
helm template ledger-next-operator helm/operator --set ledger-next-operator-crds.create=false \
  --set pvcProtection.enabled=false --api-versions "$vap" > "$work/disabled.yaml"
if grep -Eq '^kind: (CustomResourceDefinition|ValidatingAdmissionPolicy)' "$work/disabled.yaml"; then
  fail 'CRD dependency or policy disable flag ignored'
fi
# Render-time validation must continue to reject malformed exemptions/annotation keys.
if helm template t helm/operator --api-versions "$vap" \
  --set pvcProtection.allowDeletionAnnotation=bad/key/extra >/dev/null 2>&1; then
  fail 'invalid annotation accepted'
fi
if helm template t helm/operator --api-versions "$vap" \
  --set 'pvcProtection.additionalExemptServiceAccounts[0]=not-a-serviceaccount' >/dev/null 2>&1; then
  fail 'invalid exemption accepted'
fi
echo 'OK: next resources, RBAC, six CRDs, volume policies and cluster-scoped cleanup are isolated from Pebble'
