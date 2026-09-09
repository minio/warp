{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "warp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "warp.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "warp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Set the image tag to use.
*/}}
{{- define "warp.imageVersion" -}}
{{- default .Chart.AppVersion .Values.image.version -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "warp.labels" -}}
helm.sh/chart: {{ include "warp.chart" . }}
{{ include "warp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "warp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "warp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "warp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "warp.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Reject an rdma.mode warp would reject at startup, so the mistake surfaces at
install time rather than as a CrashLoopBackOff.
*/}}
{{- define "warp.rdmaValidate" -}}
{{- if .Values.rdma.enabled -}}
{{- if not (has .Values.rdma.mode (list "cpu" "gpu")) -}}
{{- fail (printf "rdma.mode must be \"cpu\" or \"gpu\", got %q" .Values.rdma.mode) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Container image to run.

The RDMA build is a separate flavor of a release rather than part of it: its own
repository and a "<version>.rdma" tag. The rolling "latest" tag deliberately
never resolves to an RDMA image, so enabling RDMA means naming a version.
*/}}
{{- define "warp.image" -}}
{{- include "warp.rdmaValidate" . -}}
{{- if .Values.rdma.enabled -}}
{{- $repo := default .Values.image.repository .Values.rdma.image.repository -}}
{{- $tag := .Values.rdma.image.version -}}
{{- if not $tag -}}
{{- $base := include "warp.imageVersion" . -}}
{{- if eq $base "latest" -}}
{{- fail "rdma.enabled needs a released version: set image.version, or rdma.image.version to name the tag outright. RDMA images are tagged <version>.rdma and there is no rolling latest.rdma." -}}
{{- end -}}
{{- $tag = printf "%s.rdma" $base -}}
{{- end -}}
{{- printf "%s:%s" $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (include "warp.imageVersion" .) -}}
{{- end -}}
{{- end -}}

{{/*
Resources for one container: its own block with rdma.resources merged over it,
which is where the fabric and GPU requests go. Emits nothing when both are empty.

Call as: include "warp.resources" (dict "ctx" $ "base" $.Values.clientResources)
*/}}
{{- define "warp.resources" -}}
{{- $res := deepCopy (default (dict) .base) -}}
{{- if .ctx.Values.rdma.enabled -}}
{{- $res = mergeOverwrite $res (deepCopy (default (dict) .ctx.Values.rdma.resources)) -}}
{{- end -}}
{{- if $res -}}
{{- toYaml $res -}}
{{- end -}}
{{- end -}}

{{/*
Container securityContext, with CAP_IPC_LOCK added for RDMA: the NIC pins the
transfer buffers it registers, and the node's default memlock limit otherwise
caps that at a few megabytes. Emits nothing when there is nothing to set.
*/}}
{{- define "warp.securityContext" -}}
{{- $sc := deepCopy (default (dict) .Values.securityContext) -}}
{{- if and .Values.rdma.enabled .Values.rdma.ipcLock -}}
{{- $add := dig "capabilities" "add" (list) $sc -}}
{{- $sc = mergeOverwrite $sc (dict "capabilities" (dict "add" (uniq (append $add "IPC_LOCK")))) -}}
{{- end -}}
{{- if $sc -}}
{{- toYaml $sc -}}
{{- end -}}
{{- end -}}

{{/*
RDMA environment for the server (the Job). WARP_RDMA and WARP_RDMA_WINDOW are
the --rdma / --rdma.window flags, so they reach warp whichever configuration
method is in use. S3RDMA_DEVICE is read by libs3rdma itself.
*/}}
{{- define "warp.rdmaServerEnv" -}}
{{- if .Values.rdma.enabled }}
- name: WARP_RDMA
  value: {{ .Values.rdma.mode | quote }}
{{- with .Values.rdma.window }}
- name: WARP_RDMA_WINDOW
  value: {{ . | quote }}
{{- end }}
{{- with .Values.rdma.device }}
- name: S3RDMA_DEVICE
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
RDMA environment for the clients (the StatefulSet). The server forwards --rdma
to every client with the rest of the benchmark flags, so the mode is not set
here; only the device selection is, because libs3rdma reads it on the host that
does the transfer.
*/}}
{{- define "warp.rdmaClientEnv" -}}
{{- if .Values.rdma.enabled }}
{{- with .Values.rdma.device }}
- name: S3RDMA_DEVICE
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}
