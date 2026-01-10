{{- define "benchmark-operator.name" -}}
benchmark-operator
{{- end -}}

{{- define "benchmark-operator.fullname" -}}
{{- printf "%s" (include "benchmark-operator.name" .) -}}
{{- end -}}

{{- define "benchmark-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
{{- else -}}
{{- include "benchmark-operator.fullname" . -}}
{{- end -}}
{{- end -}}
