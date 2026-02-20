package replication

import (
	"encoding/json"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	"github.com/formancehq/go-libs/v4/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/grpc"
)

func mapExporter(exporter ledger.Exporter) *grpc.Exporter {
	return &grpc.Exporter{
		Id: exporter.ID,
		CreatedAt: &timestamppb.Timestamp{
			Seconds: exporter.CreatedAt.Unix(),
			Nanos:   int32(exporter.CreatedAt.Nanosecond()),
		},
		Config: mapExporterConfiguration(exporter.ExporterConfiguration),
	}
}

func mapPipelineConfiguration(cfg ledger.PipelineConfiguration) *grpc.PipelineConfiguration {
	return &grpc.PipelineConfiguration{
		ExporterId: cfg.ExporterID,
		Ledger:     cfg.Ledger,
	}
}

func mapPipelineConfigurationFromGRPC(cfg *grpc.PipelineConfiguration) ledger.PipelineConfiguration {
	return ledger.PipelineConfiguration{
		ExporterID: cfg.ExporterId,
		Ledger:     cfg.Ledger,
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
		LastLogID: pipeline.LastLogID,
		Error:     pipeline.Error,
	}
}

func mapPipelineFromGRPC(pipeline *grpc.Pipeline) ledger.Pipeline {
	return ledger.Pipeline{
		PipelineConfiguration: mapPipelineConfigurationFromGRPC(pipeline.Config),
		CreatedAt:             time.New(pipeline.CreatedAt.AsTime()),
		ID:                    pipeline.Id,
		Enabled:               pipeline.Enabled,
		LastLogID:             pipeline.LastLogID,
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

func mapExporterFromGRPC(exporter *grpc.Exporter) ledger.Exporter {
	return ledger.Exporter{
		ID:                    exporter.Id,
		CreatedAt:             time.New(exporter.CreatedAt.AsTime()),
		ExporterConfiguration: mapExporterConfigurationFromGRPC(exporter.Config),
	}
}

func mapExporterConfigurationFromGRPC(from *grpc.ExporterConfiguration) ledger.ExporterConfiguration {
	return ledger.ExporterConfiguration{
		Driver: from.Driver,
		Config: json.RawMessage(from.Config),
	}
}

func mapExporterConfiguration(configuration ledger.ExporterConfiguration) *grpc.ExporterConfiguration {
	return &grpc.ExporterConfiguration{
		Driver: configuration.Driver,
		Config: string(configuration.Config),
	}
}
