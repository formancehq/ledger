{{- define "ledger-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "ledger-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "ledger-operator.labels" -}}
app.kubernetes.io/name: {{ include "ledger-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "ledger-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ledger-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Validated rbac.scope. Fails the render on an unknown value, or on
`namespaced` without a watchNamespace (the operator would still watch every
namespace while only being authorized in one).
*/}}
{{- define "ledger-operator.rbacScope" -}}
{{- $scope := .Values.rbac.scope -}}
{{- if not (has $scope (list "cluster" "namespaced")) -}}
{{- fail (printf "rbac.scope must be \"cluster\" or \"namespaced\", got %q" $scope) -}}
{{- end -}}
{{- if and (eq $scope "namespaced") (not .Values.watchNamespace) -}}
{{- fail "rbac.scope=namespaced requires watchNamespace to be set" -}}
{{- end -}}
{{- $scope -}}
{{- end }}

{{- define "ledger-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ledger-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
