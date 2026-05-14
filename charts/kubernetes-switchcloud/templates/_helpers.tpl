{{- define "kubernetes-switchcloud.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kubernetes-switchcloud.fullname" -}}
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

{{- define "kubernetes-switchcloud.credentialsSecretName" -}}
{{- if .Values.openstack.existingSecret -}}
{{ .Values.openstack.existingSecret }}
{{- else -}}
{{ .Release.Name }}-openstack-credentials
{{- end -}}
{{- end }}

{{/*
wait-for-kubeconfig init container — polls until Kamaji provisions super-admin.svc.
Deadline is 10m (below the 15m HelmRelease install timeout).
*/}}
{{- define "kubernetes-switchcloud.waitForAdminKubeconfig" -}}
- name: wait-for-kubeconfig
  image: "busybox:1.37"
  command:
  - sh
  - -c
  - |
    set -eu
    deadline=$(( $(date +%s) + 600 ))
    until [ -s /etc/kubernetes/kubeconfig/super-admin.svc ]; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "admin kubeconfig was not provisioned within 10m; exiting" >&2
        exit 1
      fi
      echo "waiting for admin kubeconfig..."
      sleep 5
    done
  volumeMounts:
  - name: kubeconfig
    mountPath: /etc/kubernetes/kubeconfig
    readOnly: true
{{- end }}
