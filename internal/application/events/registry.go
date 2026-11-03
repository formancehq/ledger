package events

import (
	"context"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

// sinkFactory creates a Sink using its captured leadership-generation context.
type sinkFactory func(context.Context, *ledgerpb.SinkConfig, Format) (Sink, error)

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
