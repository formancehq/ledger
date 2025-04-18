package replication

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mapConnector(connector ledger.Connector) *grpc.Connector {
	return &grpc.Connector{
		Id: connector.ID,
		CreatedAt: &timestamppb.Timestamp{
			Seconds: connector.CreatedAt.Unix(),
			Nanos:   int32(connector.CreatedAt.Nanosecond()),
		},
		Config: mapConnectorConfiguration(connector.ConnectorConfiguration),
	}
}

func mapPipelineConfiguration(cfg ledger.PipelineConfiguration) *grpc.PipelineConfiguration {
	return &grpc.PipelineConfiguration{
		ConnectorId: cfg.ConnectorID,
		Ledger:      cfg.Ledger,
	}
}

func mapPipelineConfigurationFromGRPC(cfg *grpc.PipelineConfiguration) ledger.PipelineConfiguration {
	return ledger.PipelineConfiguration{
		ConnectorID: cfg.ConnectorId,
		Ledger:      cfg.Ledger,
	}
}

func mapPipeline(pipeline ledger.Pipeline) *grpc.Pipeline {
	return &grpc.Pipeline{
		Config: mapPipelineConfiguration(pipeline.PipelineConfiguration),
		CreatedAt: &timestamppb.Timestamp{
			Seconds: pipeline.CreatedAt.Unix(),
			Nanos:   int32(pipeline.CreatedAt.Nanosecond()),
		},
		Id:        pipeline.ID,
		Enabled:   pipeline.Enabled,
		LastLogID: uint64(pipeline.LastLogID),
		Error:     pipeline.Error,
	}
}

func mapPipelineFromGRPC(pipeline *grpc.Pipeline) ledger.Pipeline {
	return ledger.Pipeline{
		PipelineConfiguration: mapPipelineConfigurationFromGRPC(pipeline.Config),
		CreatedAt:             time.New(pipeline.CreatedAt.AsTime()),
		ID:                    pipeline.Id,
		Enabled:               pipeline.Enabled,
		LastLogID:             int(pipeline.LastLogID),
		Error:                 pipeline.Error,
	}
}

func mapCursor[V any](ret *bunpaginate.Cursor[V]) *grpc.Cursor {
	return &grpc.Cursor{
		Next:    ret.Next,
		HasMore: ret.HasMore,
		Prev:    ret.Previous,
	}
}

func mapCursorFromGRPC[V any](ret *grpc.Cursor, data []V) *bunpaginate.Cursor[V] {
	return &bunpaginate.Cursor[V]{
		Next:     ret.Next,
		HasMore:  ret.HasMore,
		Previous: ret.Prev,
		Data:     data,
	}
}

func mapConnectorFromGRPC(connector *grpc.Connector) ledger.Connector {
	return ledger.Connector{
		ID:                     connector.Id,
		CreatedAt:              time.New(connector.CreatedAt.AsTime()),
		ConnectorConfiguration: mapConnectorConfigurationFromGRPC(connector.Config),
	}
}

func mapConnectorConfigurationFromGRPC(from *grpc.ConnectorConfiguration) ledger.ConnectorConfiguration {
	return ledger.ConnectorConfiguration{
		Driver: from.Driver,
		Config: json.RawMessage(from.Config),
	}
}

func mapConnectorConfiguration(configuration ledger.ConnectorConfiguration) *grpc.ConnectorConfiguration {
	return &grpc.ConnectorConfiguration{
		Driver: configuration.Driver,
		Config: string(configuration.Config),
	}
}
