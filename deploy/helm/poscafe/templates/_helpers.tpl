{{- define "poscafe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- define "poscafe.fullname" -}}
{{- default (include "poscafe.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
