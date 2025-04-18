package replication

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/grpc"
)

type ThroughGRPCReplicationBackend struct {
	client grpc.ReplicationClient
}

func (t ThroughGRPCReplicationBackend) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	ret, err := t.client.ListConnectors(ctx, &grpc.ListConnectorsRequest{})
	if err != nil {
		return nil, err
	}

	return mapCursorFromGRPC(ret.Cursor, Map(ret.Data, mapConnectorFromGRPC)), nil
}

func (t ThroughGRPCReplicationBackend) CreateConnector(ctx context.Context, configuration ledger.ConnectorConfiguration) (*ledger.Connector, error) {
	connector, err := t.client.CreateConnector(ctx, &grpc.CreateConnectorRequest{
		Config: mapConnectorConfiguration(configuration),
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapConnectorFromGRPC(connector.Connector)), nil
}

func (t ThroughGRPCReplicationBackend) DeleteConnector(ctx context.Context, id string) error {
	_, err := t.client.DeleteConnector(ctx, &grpc.DeleteConnectorRequest{
		Id: id,
	})
	return err
}

func (t ThroughGRPCReplicationBackend) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	connector, err := t.client.GetConnector(ctx, &grpc.GetConnectorRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapConnectorFromGRPC(connector.Connector)), nil
}

func (t ThroughGRPCReplicationBackend) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	pipelines, err := t.client.ListPipelines(ctx, &grpc.ListPipelinesRequest{})
	if err != nil {
		return nil, err
	}

	return mapCursorFromGRPC(pipelines.Cursor, Map(pipelines.Data, mapPipelineFromGRPC)), nil
}

func (t ThroughGRPCReplicationBackend) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	pipeline, err := t.client.GetPipeline(ctx, &grpc.GetPipelineRequest{
		Id: id,
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapPipelineFromGRPC(pipeline.Pipeline)), nil
}

func (t ThroughGRPCReplicationBackend) CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	pipeline, err := t.client.CreatePipeline(ctx, &grpc.CreatePipelineRequest{
		Config: mapPipelineConfiguration(pipelineConfiguration),
	})
	if err != nil {
		return nil, err
	}

	return pointer.For(mapPipelineFromGRPC(pipeline.Pipeline)), nil
}

func (t ThroughGRPCReplicationBackend) DeletePipeline(ctx context.Context, id string) error {
	_, err := t.client.DeletePipeline(ctx, &grpc.DeletePipelineRequest{
		Id: id,
	})
	return err
}

func (t ThroughGRPCReplicationBackend) StartPipeline(ctx context.Context, id string) error {
	_, err := t.client.StartPipeline(ctx, &grpc.StartPipelineRequest{
		Id: id,
	})
	return err
}

func (t ThroughGRPCReplicationBackend) ResetPipeline(ctx context.Context, id string) error {
	_, err := t.client.ResetPipeline(ctx, &grpc.ResetPipelineRequest{
		Id: id,
	})
	return err
}

func (t ThroughGRPCReplicationBackend) StopPipeline(ctx context.Context, id string) error {
	_, err := t.client.StopPipeline(ctx, &grpc.StopPipelineRequest{
		Id: id,
	})
	return err
}

var _ system.ReplicationBackend = (*ThroughGRPCReplicationBackend)(nil)

func NewThroughGRPCReplicationBackend(client grpc.ReplicationClient) *ThroughGRPCReplicationBackend {
	return &ThroughGRPCReplicationBackend{
		client: client,
	}
}
