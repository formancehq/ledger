package server

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// pyroscopeResourceTags maps the OTel resource attributes that identify a
// Ledger cluster and node to the Pyroscope tag carrying the same value.
// Pyroscope label names cannot contain dots, so the tags use the
// underscore form the OTLP→Prometheus collector gives these attributes
// on metrics: profiles and metrics can then be filtered with the same
// namespace, cluster and node dashboard variables.
var pyroscopeResourceTags = map[attribute.Key]string{
	"k8s.namespace.name":         "k8s_namespace_name",
	resourceAttributeClusterName: "formance_ledger_cluster_name",
	resourceAttributeNodeID:      "formance_ledger_node_id",
}

// addPyroscopeResourceTags copies the cluster and node identity of the
// telemetry resource into the Pyroscope tags. Attributes absent from the
// resource add no tag. A resource value overrides a --pyroscope-tags
// entry of the same name so profiles always match the metrics.
func addPyroscopeResourceTags(tags map[string]string, res *resource.Resource) {
	attributes := res.Set()
	for key, tag := range pyroscopeResourceTags {
		if value, ok := attributes.Value(key); ok {
			tags[tag] = value.String()
		}
	}
}
