package replication

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ThroughGRPCBackend struct {
	client grpc.ReplicationClient
}

func (t ThroughGRPCBackend) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	ret, err := t.client.ListConnectors(ctx, &grpc.ListConnectorsRequest{})
	if err != nil {
		return nil, err
	}

	return mapCursorFromGRPC(ret.Cursor, Map(ret.Data, mapConnectorFromGRPC)), nil
}

func (t ThroughGRPCBackend) CreateConnector(ctx context.Context, configuration ledger.ConnectorConfiguration) (*ledger.Connector, error) {
	connector, err := t.client.CreateConnector(ctx, &grpc.CreateConnectorRequest{
		Config: mapConnectorConfiguration(configuration),
	})
	if err != nil {
		if status.Code(err) != codes.InvalidArgument {
			return nil, system.NewErrInvalidDriverConfiguration(configuration.Driver, err)
		}

		return nil, err
	}

	return pointer.For(mapConnectorFromGRPC(connector.Connector)), nil
}

func (t ThroughGRPCBackend) DeleteConnector(ctx context.Context, id string) error {
	_, err := t.client.DeleteConnector(ctx, &grpc.DeleteConnectorRequest{
		Id: id,
	})
	if err != nil && status.Code(err) == codes.NotFound {
		return system.NewErrConnectorNotFound(id)
	}
	return err
}

func (t ThroughGRPCBackend) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	connector, err := t.client.GetConnector(ctx, &grpc.GetConnectorRequest{
		Id: id,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, system.NewErrConnectorNotFound(id)
		}
		return nil, err
	}

	return pointer.For(mapConnectorFromGRPC(connector.Connector)), nil
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
