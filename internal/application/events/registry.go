package events

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// sinkFactory creates a Sink from a SinkConfig and a format.
type sinkFactory func(*ledgerpb.SinkConfig, Format) (Sink, error)

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
func sinkTypeName(sc *ledgerpb.SinkConfig) string {
	switch sc.GetType().(type) {
	case *ledgerpb.SinkConfig_Kafka:
		return "kafka"
	case *ledgerpb.SinkConfig_Nats:
		return "nats"
	case *ledgerpb.SinkConfig_Clickhouse:
		return "clickhouse"
	case *ledgerpb.SinkConfig_Databricks:
		return "databricks"
	case *ledgerpb.SinkConfig_Http:
		return "http"
	default:
		return ""
	}
}
