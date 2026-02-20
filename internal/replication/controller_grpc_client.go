package replication

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v4/collectionutils"
	"github.com/formancehq/go-libs/v4/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/grpc"
)

type ThroughGRPCBackend struct {
	client grpc.ReplicationClient
}

func (t ThroughGRPCBackend) ListExporters(ctx context.Context) (*bunpaginate.Cursor[ledger.Exporter], error) {
	ret, err := t.client.ListExporters(ctx, &grpc.ListExportersRequest{})
	if err != nil {
		return nil, err
	}

	return mapCursorFromGRPC(ret.Cursor, Map(ret.Data, mapExporterFromGRPC)), nil
}

func (t ThroughGRPCBackend) CreateExporter(ctx context.Context, configuration ledger.ExporterConfiguration) (*ledger.Exporter, error) {
	exporter, err := t.client.CreateExporter(ctx, &grpc.CreateExporterRequest{
		Config: mapExporterConfiguration(configuration),
	})
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			return nil, system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
		}

		return nil, err
	}

	return pointer.For(mapExporterFromGRPC(exporter.Exporter)), nil
}

func (t ThroughGRPCBackend) UpdateExporter(ctx context.Context, id string, configuration ledger.ExporterConfiguration) error {
	_, err := t.client.UpdateExporter(ctx, &grpc.UpdateExporterRequest{
		Id:     id,
		Config: mapExporterConfiguration(configuration),
	})
	if err != nil {
		switch {
		case status.Code(err) == codes.InvalidArgument:
			return system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
		case status.Code(err) == codes.NotFound:
			return system.NewErrExporterNotFound(id)
		default:
			return err
		}
	}
	return nil
}

func (t ThroughGRPCBackend) DeleteExporter(ctx context.Context, id string) error {
	_, err := t.client.DeleteExporter(ctx, &grpc.DeleteExporterRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		return system.NewErrExporterNotFound(id)
	}
	return err
}

func (t ThroughGRPCBackend) GetExporter(ctx context.Context, id string) (*ledger.Exporter, error) {
	exporter, err := t.client.GetExporter(ctx, &grpc.GetExporterRequest{
		Id: id,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, system.NewErrExporterNotFound(id)
		}
		return nil, err
	}

	return pointer.For(mapExporterFromGRPC(exporter.Exporter)), nil
}

func (t ThroughGRPCBackend) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	pipelines, err := t.client.ListPipelines(ctx, &grpc.ListPipelinesRequest{})
	if err != nil {
		return nil, err
	}

	return mapCursorFromGRPC(pipelines.Cursor, Map(pipelines.Data, mapPipelineFromGRPC)), nil
}

func (t ThroughGRPCBackend) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	pipeline, err := t.client.GetPipeline(ctx, &grpc.GetPipelineRequest{
		Id: id,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, ledger.NewErrPipelineNotFound(id)
		}
		return nil, err
	}

	return pointer.For(mapPipelineFromGRPC(pipeline.Pipeline)), nil
}

func (t ThroughGRPCBackend) CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	pipeline, err := t.client.CreatePipeline(ctx, &grpc.CreatePipelineRequest{
		Config: mapPipelineConfiguration(pipelineConfiguration),
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapPipelineFromGRPC(pipeline.Pipeline)), nil
}

func (t ThroughGRPCBackend) DeletePipeline(ctx context.Context, id string) error {
	_, err := t.client.DeletePipeline(ctx, &grpc.DeletePipelineRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

func (t ThroughGRPCBackend) StartPipeline(ctx context.Context, id string) error {
	_, err := t.client.StartPipeline(ctx, &grpc.StartPipelineRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.FailedPrecondition {
		return ledger.NewErrAlreadyStarted(id)
	}
	return err
}

func (t ThroughGRPCBackend) ResetPipeline(ctx context.Context, id string) error {
	_, err := t.client.ResetPipeline(ctx, &grpc.ResetPipelineRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

func (t ThroughGRPCBackend) StopPipeline(ctx context.Context, id string) error {
	_, err := t.client.StopPipeline(ctx, &grpc.StopPipelineRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		return ledger.NewErrPipelineNotFound(id)
	}
	return err
}

var _ system.ReplicationBackend = (*ThroughGRPCBackend)(nil)

func NewThroughGRPCBackend(client grpc.ReplicationClient) *ThroughGRPCBackend {
	return &ThroughGRPCBackend{
		client: client,
	}
}
