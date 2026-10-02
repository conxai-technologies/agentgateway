{{- define "agentgateway-standalone.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentgateway-standalone.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "agentgateway-standalone.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace }}
{{- end }}

{{- define "agentgateway-standalone.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentgateway-standalone.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agentgateway-standalone.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: standalone
{{- end }}

{{- define "agentgateway-standalone.labels" -}}
helm.sh/chart: {{ include "agentgateway-standalone.chart" . }}
{{ include "agentgateway-standalone.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "agentgateway-standalone.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "agentgateway-standalone.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "agentgateway-standalone.imageTag" -}}
{{- $tag := . -}}
{{- if hasPrefix "v" $tag -}}
{{- $tag -}}
{{- else if regexMatch "^[0-9]+\\.[0-9]+\\..*$" $tag -}}
{{- printf "v%s" $tag -}}
{{- else -}}
{{- $tag -}}
{{- end -}}
{{- end }}

{{- define "agentgateway-standalone.mainImage" -}}
{{- if kindIs "string" .Values.image -}}
{{- required "image must not be empty" .Values.image -}}
{{- else if kindIs "map" .Values.image -}}
{{- $tag := include "agentgateway-standalone.imageTag" (.Values.image.tag | default .Chart.AppVersion) -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository $tag -}}
{{- else -}}
{{- fail "image must be a string or mapping" -}}
{{- end -}}
{{- end }}

{{- define "agentgateway-standalone.mainImagePullPolicy" -}}
{{- if kindIs "string" .Values.image -}}
IfNotPresent
{{- else if kindIs "map" .Values.image -}}
{{- .Values.image.pullPolicy | default "IfNotPresent" -}}
{{- else -}}
{{- fail "image must be a string or mapping" -}}
{{- end -}}
{{- end }}

{{- define "agentgateway-standalone.serviceSpecFields" -}}
{{- with .clusterIP }}
clusterIP: {{ . }}
{{- end }}
{{- with .clusterIPs }}
clusterIPs:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .externalIPs }}
externalIPs:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .externalName }}
externalName: {{ . }}
{{- end }}
{{- with .loadBalancerIP }}
loadBalancerIP: {{ . }}
{{- end }}
{{- with .loadBalancerSourceRanges }}
loadBalancerSourceRanges:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .loadBalancerClass }}
loadBalancerClass: {{ . }}
{{- end }}
{{- with .externalTrafficPolicy }}
externalTrafficPolicy: {{ . }}
{{- end }}
{{- with .internalTrafficPolicy }}
internalTrafficPolicy: {{ . }}
{{- end }}
{{- if not (kindIs "invalid" .healthCheckNodePort) }}
healthCheckNodePort: {{ .healthCheckNodePort }}
{{- end }}
{{- with .sessionAffinity }}
sessionAffinity: {{ . }}
{{- end }}
{{- with .sessionAffinityConfig }}
sessionAffinityConfig:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .ipFamilies }}
ipFamilies:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .ipFamilyPolicy }}
ipFamilyPolicy: {{ . }}
{{- end }}
{{- if .publishNotReadyAddresses }}
publishNotReadyAddresses: true
{{- end }}
{{- if not (kindIs "invalid" .allocateLoadBalancerNodePorts) }}
allocateLoadBalancerNodePorts: {{ .allocateLoadBalancerNodePorts }}
{{- end }}
{{- with .trafficDistribution }}
trafficDistribution: {{ . }}
{{- end }}
{{- end }}

{{- define "agentgateway-standalone.baseConfig" -}}
{{- if .Values.config -}}
{{ toYaml .Values.config }}
{{- else -}}
gateways:
  default:
    port: 4000
ui: {}
llm:
  models: []
mcp:
  targets: []
{{- end -}}
{{- end }}

{{- define "agentgateway-standalone.renderedConfig" -}}
{{- $renderedConfig := include "agentgateway-standalone.baseConfig" . | fromYaml -}}
{{- if not (kindIs "map" $renderedConfig) -}}
{{- fail "config must render to a YAML mapping" -}}
{{- end -}}
{{- $config := get $renderedConfig "config" | default dict -}}
{{- if not (kindIs "map" $config) -}}
{{- fail "config.config must be a YAML mapping" -}}
{{- end -}}
{{- $database := include "agentgateway-standalone.databaseConfig" . | fromYaml -}}
{{- if $database -}}
{{- if hasKey $config "database" -}}
{{- fail "set the database connection with the 'database' value, not 'config.config.database'" -}}
{{- end -}}
{{- $_ := set $config "database" $database -}}
{{- end -}}
{{- if eq .Values.mode "database" -}}
{{- $_ := set $config "storage" (dict "mode" "hybrid") -}}
{{- else -}}
{{- $_ := set $config "storage" (dict "mode" "readOnly") -}}
{{- end }}
{{- $_ := set $renderedConfig "config" $config -}}
{{ toYaml $renderedConfig }}
{{- end }}

{{/*
The config.database section for the 'database' value, or nothing when no database is set.
A connection string from an existing secret is referenced through an environment variable,
which agentgateway expands when it reads the config file.
*/}}
{{- define "agentgateway-standalone.databaseConfig" -}}
{{- $postgres := .Values.database.postgres -}}
{{- if $postgres.existingSecret.name }}
url: ${AGENTGATEWAY_DATABASE_URL}
{{- else if $postgres.url }}
url: {{ $postgres.url | quote }}
{{- end }}
{{- if and (or $postgres.existingSecret.name $postgres.url) (not (kindIs "invalid" .Values.database.maxConnections)) }}
maxConnections: {{ int .Values.database.maxConnections }}
{{- end }}
{{- end }}

{{/*
Hash only configuration that agentgateway reads at startup. All other sections of the
rendered configuration, plus config.modelCatalog, are reloaded without restarting the pod.
*/}}
{{- define "agentgateway-standalone.startupConfig" -}}
{{- $renderedConfig := include "agentgateway-standalone.renderedConfig" . | fromYaml -}}
{{- $config := get $renderedConfig "config" | default dict | deepCopy -}}
{{- $_ := unset $config "modelCatalog" -}}
{{ toYaml $config }}
{{- end }}

{{- define "agentgateway-standalone.validate" -}}
{{- $mode := .Values.mode -}}
{{- if not (has $mode (list "readonly" "database")) -}}
{{- fail (printf "mode must be one of: readonly, database (got %q)" $mode) -}}
{{- end -}}
{{- $postgresUrl := .Values.database.postgres.url | default "" -}}
{{- $secretName := .Values.database.postgres.existingSecret.name | default "" -}}
{{- if and $postgresUrl $secretName -}}
{{- fail "set only one of database.postgres.url and database.postgres.existingSecret.name" -}}
{{- end -}}
{{- if and $postgresUrl (not (regexMatch "^postgres(ql)?://" $postgresUrl)) -}}
{{- fail (printf "database.postgres.url must start with postgres:// or postgresql:// (got %q)" $postgresUrl) -}}
{{- end -}}
{{- if and $secretName (not .Values.database.postgres.existingSecret.key) -}}
{{- fail "database.postgres.existingSecret.key must not be empty" -}}
{{- end -}}
{{- if and (eq $mode "database") (not (or $postgresUrl $secretName)) -}}
{{- fail "mode=database requires database.postgres.url or database.postgres.existingSecret.name" -}}
{{- end -}}
{{- $maxConnections := .Values.database.maxConnections -}}
{{- if not (kindIs "invalid" $maxConnections) -}}
{{- if not (or $postgresUrl $secretName) -}}
{{- fail "database.maxConnections requires database.postgres.url or database.postgres.existingSecret.name" -}}
{{- end -}}
{{- $minConnections := ternary 2 1 (eq $mode "database") -}}
{{- if lt (int $maxConnections) $minConnections -}}
{{- fail (printf "database.maxConnections must be at least %d when mode=%s (got %v)" $minConnections $mode $maxConnections) -}}
{{- end -}}
{{- end -}}
{{- end -}}
