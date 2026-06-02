#!/usr/bin/env bash
# Sync controller-gen RBAC rules into the Helm chart ClusterRole template.
# Run automatically by `just generate`.
set -euo pipefail

cd "$(dirname "$0")/.."

dest="helm/operator/templates/clusterrole.yaml"
src="config/rbac/role.yaml"

# Write Helm template header
printf '%s\n' \
  'apiVersion: rbac.authorization.k8s.io/v1' \
  'kind: ClusterRole' \
  'metadata:' \
  '  name: {{ include "ledger-operator.fullname" . }}' \
  '  labels:' \
  '    {{- include "ledger-operator.labels" . | nindent 4 }}' \
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
