#!/usr/bin/env bash
# Sync controller-gen RBAC rules into the Helm chart ClusterRole template.
# Run automatically by `just generate`.
set -euo pipefail

cd "$(dirname "$0")/.."

dest="chart/templates/clusterrole.yaml"
src="config/rbac/role.yaml"

# Write Helm template header
printf '%s\n' \
  '{{- if .Values.rbac.create }}' \
  'apiVersion: rbac.authorization.k8s.io/v1' \
  'kind: ClusterRole' \
  'metadata:' \
  '  name: {{ include "benchmark-operator.fullname" . }}' \
  '  labels:' \
  '    app.kubernetes.io/name: {{ include "benchmark-operator.name" . }}' \
  '    app.kubernetes.io/instance: {{ .Release.Name }}' \
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
{{- end }}
EOF
