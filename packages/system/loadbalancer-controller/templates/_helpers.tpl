{{- define "loadbalancer-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "loadbalancer-controller.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- default .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{- define "loadbalancer-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "loadbalancer-controller.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{- define "loadbalancer-controller.labels" -}}
app.kubernetes.io/name: {{ include "loadbalancer-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "loadbalancer-controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "loadbalancer-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
