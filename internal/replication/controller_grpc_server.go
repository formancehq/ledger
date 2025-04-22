package replication

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/formancehq/go-libs/v3/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServiceImpl struct {
	grpc.UnimplementedReplicationServer
	manager *Manager
}

func (srv GRPCServiceImpl) CreateConnector(ctx context.Context, request *grpc.CreateConnectorRequest) (*grpc.CreateConnectorResponse, error) {
	connector, err := srv.manager.CreateConnector(ctx, ledger.ConnectorConfiguration{
		Driver: request.Config.Driver,
		Config: json.RawMessage(request.Config.Config),
	})
	if err != nil {
		switch {
		case errors.Is(err, system.ErrInvalidDriverConfiguration{}):
			err := &system.ErrInvalidDriverConfiguration{}
			if !errors.As(err, &err) {
				panic("should never happen")
			}

			return nil, status.Errorf(codes.InvalidArgument, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.CreateConnectorResponse{
		Connector: mapConnector(*connector),
	}, nil
}

func (srv GRPCServiceImpl) ListConnectors(ctx context.Context, _ *grpc.ListConnectorsRequest) (*grpc.ListConnectorsResponse, error) {
	ret, err := srv.manager.ListConnectors(ctx)
	if err != nil {
		return nil, err
	}

	return &grpc.ListConnectorsResponse{
		Data:   collectionutils.Map(ret.Data, mapConnector),
		Cursor: mapCursor(ret),
	}, nil
}

func (srv GRPCServiceImpl) GetConnector(ctx context.Context, request *grpc.GetConnectorRequest) (*grpc.GetConnectorResponse, error) {
	ret, err := srv.manager.GetConnector(ctx, request.Id)
	if err != nil {
		switch {
		case errors.Is(err, system.ErrConnectorNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.GetConnectorResponse{
		Connector: mapConnector(*ret),
	}, nil
}

func (srv GRPCServiceImpl) DeleteConnector(ctx context.Context, request *grpc.DeleteConnectorRequest) (*grpc.DeleteConnectorResponse, error) {
	if err := srv.manager.DeleteConnector(ctx, request.Id); err != nil {
		switch {
		case errors.Is(err, system.ErrConnectorNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}
	return &grpc.DeleteConnectorResponse{}, nil
}

func (srv GRPCServiceImpl) ListPipelines(ctx context.Context, _ *grpc.ListPipelinesRequest) (*grpc.ListPipelinesResponse, error) {
	cursor, err := srv.manager.ListPipelines(ctx)
	if err != nil {
		return nil, err
	}

	return &grpc.ListPipelinesResponse{
		Data:   collectionutils.Map(cursor.Data, mapPipeline),
		Cursor: mapCursor(cursor),
	}, nil
}

func (srv GRPCServiceImpl) GetPipeline(ctx context.Context, request *grpc.GetPipelineRequest) (*grpc.GetPipelineResponse, error) {
	pipeline, err := srv.manager.GetPipeline(ctx, request.Id)
	if err != nil {
		switch {
		case errors.Is(err, ledger.ErrPipelineNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.GetPipelineResponse{
		Pipeline: mapPipeline(*pipeline),
	}, nil
}

func (srv GRPCServiceImpl) CreatePipeline(ctx context.Context, request *grpc.CreatePipelineRequest) (*grpc.CreatePipelineResponse, error) {
	pipeline, err := srv.manager.CreatePipeline(ctx, mapPipelineConfigurationFromGRPC(request.Config))
	if err != nil {
		return nil, err
	}

	return &grpc.CreatePipelineResponse{
		Pipeline: mapPipeline(*pipeline),
	}, nil
}

func (srv GRPCServiceImpl) DeletePipeline(ctx context.Context, request *grpc.DeletePipelineRequest) (*grpc.DeletePipelineResponse, error) {
	if err := srv.manager.DeletePipeline(ctx, request.Id); err != nil {
		switch {
		case errors.Is(err, ledger.ErrPipelineNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}
	return &grpc.DeletePipelineResponse{}, nil
}

func (srv GRPCServiceImpl) StartPipeline(ctx context.Context, request *grpc.StartPipelineRequest) (*grpc.StartPipelineResponse, error) {
	if err := srv.manager.StartPipeline(ctx, request.Id); err != nil {
		switch {
		case errors.Is(err, ledger.ErrAlreadyStarted("")):
			return nil, status.Errorf(codes.FailedPrecondition, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.StartPipelineResponse{}, nil
}

func (srv GRPCServiceImpl) StopPipeline(ctx context.Context, request *grpc.StopPipelineRequest) (*grpc.StopPipelineResponse, error) {
	err := srv.manager.StopPipeline(ctx, request.Id)
	if err != nil {
		switch {
		case errors.Is(err, ledger.ErrPipelineNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.StopPipelineResponse{}, nil
}

func (srv GRPCServiceImpl) ResetPipeline(ctx context.Context, request *grpc.ResetPipelineRequest) (*grpc.ResetPipelineResponse, error) {
	if err := srv.manager.ResetPipeline(ctx, request.Id); err != nil {
		switch {
		case errors.Is(err, ledger.ErrPipelineNotFound("")):
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		default:
			return nil, err
		}
	}

	return &grpc.ResetPipelineResponse{}, nil
}

var _ grpc.ReplicationServer = (*GRPCServiceImpl)(nil)

func NewReplicationServiceImpl(runner *Manager) *GRPCServiceImpl {
	return &GRPCServiceImpl{
		manager: runner,
	}
}
