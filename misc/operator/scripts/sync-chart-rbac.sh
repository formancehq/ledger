#!/usr/bin/env bash
# Sync controller-gen RBAC rules into the Helm chart ClusterRole template.
# Run automatically by `just generate`.
set -euo pipefail

cd "$(dirname "$0")/.."

dest="helm/operator/templates/clusterrole.yaml"
src="config/rbac/role.yaml"

# Refuse stale generated rules before touching the chart. Regenerate from the
# next API/controller markers first; never rewrite the old group's permissions.
if ! grep -Fq 'ledger-next.formance.com' "$src" || grep -Fq 'ledger.formance.com' "$src"; then
  echo "Regenerate config/rbac/role.yaml for ledger-next.formance.com before syncing" >&2
  exit 1
fi

# Write Helm template header
printf '%s\n' \
  'apiVersion: rbac.authorization.k8s.io/v1' \
  'kind: ClusterRole' \
  'metadata:' \
  '  name: {{ include "ledger-next-operator.fullname" . }}' \
  '  labels:' \
  '    {{- include "ledger-next-operator.labels" . | nindent 4 }}' \
  > "$dest"

# Extract rules from controller-gen output (everything from "rules:" onward)
sed -n '/^rules:/,$p' "$src" >> "$dest"

# Append static rules not covered by controller-gen annotations
cat >> "$dest" << 'EOF'
# -- Leader election
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
# -- Event recording
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "patch"]
# -- Traefik IngressRoutes
- apiGroups: ["traefik.io"]
  resources: ["ingressroutes"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
EOF
