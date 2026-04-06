package events

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// sinkFactory creates a Sink from a SinkConfig and a format.
type sinkFactory func(*commonpb.SinkConfig, Format) (Sink, error)

// sinkFactories maps sink type names to their factory functions.
// Optional sinks (Kafka, NATS, ClickHouse, Databricks) register themselves
// via init() when their build tag is active.
var sinkFactories = map[string]sinkFactory{}

// registerSinkFactory registers a factory for the given sink type name.
// Called from init() functions in build-tagged sink files.
func registerSinkFactory(name string, fn sinkFactory) {
	sinkFactories[name] = fn
}

// sinkTypeName returns the registry key for a SinkConfig's type.
func sinkTypeName(sc *commonpb.SinkConfig) string {
	switch sc.GetType().(type) {
	case *commonpb.SinkConfig_Kafka:
		return "kafka"
	case *commonpb.SinkConfig_Nats:
		return "nats"
	case *commonpb.SinkConfig_Clickhouse:
		return "clickhouse"
	case *commonpb.SinkConfig_Databricks:
		return "databricks"
	case *commonpb.SinkConfig_Http:
		return "http"
	default:
		return ""
	}
}
