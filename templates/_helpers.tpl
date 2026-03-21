{{/*
Expand the name of the chart.
*/}}
{{- define "ledger.fullname" -}}
{{- include "core.fullname" . -}}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ledger.labels" -}}
{{- include "core.labels" . -}}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ledger.selectorLabels" -}}
{{- include "core.selectorLabels" . -}}
{{- end }}
