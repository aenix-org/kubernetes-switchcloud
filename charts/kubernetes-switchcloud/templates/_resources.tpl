{{/*
Convert a CPU quantity string (e.g. "100m", "0.5", "1", "4") to millicores integer string.
*/}}
{{- define "kubernetes-switchcloud.cpuToMillicores" -}}
{{-   $str := . | toString -}}
{{-   if hasSuffix "m" $str -}}
{{-     trimSuffix "m" $str -}}
{{-   else -}}
{{-     mulf ($str | float64) 1000.0 | int | toString -}}
{{-   end -}}
{{- end -}}

{{/*
Convert a memory quantity string to bytes (float64 string).
Supports: Ki, Mi, Gi, Ti — plain integer treated as bytes.
*/}}
{{- define "kubernetes-switchcloud.memoryToBytes" -}}
{{-   $str := . | toString -}}
{{-   if hasSuffix "Ki" $str -}}
{{-     mulf (trimSuffix "Ki" $str | float64) 1024.0 | toString -}}
{{-   else if hasSuffix "Mi" $str -}}
{{-     mulf (trimSuffix "Mi" $str | float64) 1048576.0 | toString -}}
{{-   else if hasSuffix "Gi" $str -}}
{{-     mulf (trimSuffix "Gi" $str | float64) 1073741824.0 | toString -}}
{{-   else if hasSuffix "Ti" $str -}}
{{-     mulf (trimSuffix "Ti" $str | float64) 1099511627776.0 | toString -}}
{{-   else -}}
{{-     $str | float64 | toString -}}
{{-   end -}}
{{- end -}}
