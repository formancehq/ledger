{{/*
Expand the name of the chart.
*/}}
{{- define "k6-run.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "k6-run.fullname" -}}
{{- if .Values.metadata.name }}
{{- .Values.metadata.name | replace "_" "-" }}
{{- else }}
{{- printf "k6-run-%s" .Values.script | replace "_" "-" }}
{{- end }}
{{- end }}

{{/*
Create the ConfigMap name.
*/}}
{{- define "k6-run.configmapName" -}}
{{- if .Values.metadata.name }}
{{- .Values.metadata.name | replace "_" "-" }}
{{- else }}
{{- printf "k6-archive-%s" (.Values.script | replace "_" "-") }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "k6-run.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}
