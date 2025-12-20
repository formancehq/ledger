package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SystemServiceServerImpl struct {
	service.UnimplementedSystemServiceServer
	logger     logging.Logger
	systemNode *system.Node
}

func NewSystemServiceServer(logger logging.Logger, cluster *system.Node) service.SystemServiceServer {
	return &SystemServiceServerImpl{
		logger: logger.WithFields(map[string]any{
			"service": "system.grpc-server",
		}),
		systemNode: cluster,
	}
}

func (impl *SystemServiceServerImpl) CreateLedger(ctx context.Context, req *service.CreateLedgerRequest) (*service.CreateLedgerResponse, error) {
	impl.logger.
		WithFields(map[string]any{"name": req.Name, "driver": req.Driver}).
		Infof("CreateLedger request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	if req.Driver == "" {
		return nil, fmt.Errorf("ledger driver is required")
	}

	// Convert protobuf Struct to map[string]interface{}
	config := make(map[string]interface{})
	if req.Config != nil {
		config = req.Config.AsMap()
	}

	// Convert metadata
	var md map[string]string
	if req.Metadata != nil {
		mdMap := req.Metadata.AsMap()
		md = make(map[string]string)
		for k, v := range mdMap {
			if str, ok := v.(string); ok {
				md[k] = str
			}
		}
	}

	var snapshotThreshold *uint64
	if req.SnapshotThreshold > 0 {
		snapshotThreshold = &req.SnapshotThreshold
	}

	ledgerInfo, err := impl.systemNode.CreateLedger(ctx, req.Name, req.Driver, config, md, snapshotThreshold)
	if err != nil {
		return nil, fmt.Errorf("creating ledger: %w", err)
	}

	// Convert json.RawMessage to map[string]interface{} for protobuf conversion
	var configMap map[string]interface{}
	if err := json.Unmarshal(ledgerInfo.Config, &configMap); err != nil {
		return nil, fmt.Errorf("unmarshaling ledger config: %w", err)
	}

	cfg, err := structpb.NewStruct(configMap)
	if err != nil {
		return nil, fmt.Errorf("converting ledger config to protobuf Struct: %w", err)
	}

	// Convert metadata
	var mdStruct *structpb.Struct
	if len(ledgerInfo.Metadata) > 0 {
		mdMap := make(map[string]interface{})
		for k, v := range ledgerInfo.Metadata {
			mdMap[k] = v
		}
		mdStruct, err = structpb.NewStruct(mdMap)
		if err != nil {
			return nil, fmt.Errorf("converting ledger metadata to protobuf Struct: %w", err)
		}
	}

	resp := &service.CreateLedgerResponse{
		Id:        ledgerInfo.ID,
		Name:      ledgerInfo.Name,
		Config:    cfg,
		Driver:    ledgerInfo.Driver,
		Metadata:  mdStruct,
		CreatedAt: timestamppb.New(ledgerInfo.CreatedAt.Time),
	}
	if ledgerInfo.SnapshotThreshold > 0 {
		resp.SnapshotThreshold = ledgerInfo.SnapshotThreshold
	}
	return resp, nil
}

func (impl *SystemServiceServerImpl) DeleteLedger(ctx context.Context, req *service.DeleteLedgerRequest) (*service.DeleteLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("DeleteLedger request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	if err := impl.systemNode.DeleteLedger(ctx, req.Name); err != nil {
		return nil, fmt.Errorf("deleting ledger: %w", err)
	}

	return &service.DeleteLedgerResponse{
		Message: "Ledger deleted successfully",
	}, nil
}

func (impl *SystemServiceServerImpl) Snapshot(ctx context.Context, req *service.SnapshotRequest) (*service.SnapshotResponse, error) {
	impl.logger.Debugf("Snapshot request received")
	if err := impl.systemNode.Snapshot(ctx); err != nil {
		return nil, fmt.Errorf("snapshotting cluster: %w", err)
	}
	return &service.SnapshotResponse{Message: "Snapshotting completed successfully"}, nil
}

func (impl *SystemServiceServerImpl) ResolveLedger(ctx context.Context, req *service.ResolveLedgerRequest) (*service.ResolveLedgerResponse, error) {
	impl.logger.
		WithFields(map[string]any{"ledger_name": req.LedgerName}).
		Debugf("ResolveLedger request received")

	if req.LedgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerName, ledgerID, err := impl.systemNode.ResolveLedger(ctx, req.LedgerName)
	if err != nil {
		if errors.Is(err, &ledger.NotFoundError{}) {
			return nil, status.New(codes.NotFound, err.Error()).Err()
		}
		return nil, fmt.Errorf("resolving ledger '%s': %w", req.LedgerName, err)
	}

	return &service.ResolveLedgerResponse{
		LedgerName: ledgerName,
		LedgerId:   ledgerID,
	}, nil
}

func (impl *SystemServiceServerImpl) GetAllLedgersInfo(ctx context.Context, req *service.GetAllLedgersRequest) (*service.GetAllLedgersResponse, error) {
	impl.logger.Debugf("GetAllLedgersInfo request received")

	ledgers := impl.systemNode.GetAllLedgersInfo(ctx)

	// Convert map[string]ledger.LedgerInfo to []CreateLedgerResponse
	ledgersList := make([]*service.CreateLedgerResponse, 0, len(ledgers))
	for _, ledgerInfo := range ledgers {
		// Convert json.RawMessage to map[string]interface{} for protobuf conversion
		var configMap map[string]interface{}
		if len(ledgerInfo.Config) > 0 {
			if err := json.Unmarshal(ledgerInfo.Config, &configMap); err != nil {
				return nil, fmt.Errorf("unmarshaling ledger config for '%s': %w", ledgerInfo.Name, err)
			}
		}

		cfg, err := structpb.NewStruct(configMap)
		if err != nil {
			return nil, fmt.Errorf("converting ledger config to protobuf Struct for '%s': %w", ledgerInfo.Name, err)
		}

		// Convert metadata
		var mdStruct *structpb.Struct
		if len(ledgerInfo.Metadata) > 0 {
			mdMap := make(map[string]interface{})
			for k, v := range ledgerInfo.Metadata {
				mdMap[k] = v
			}
			mdStruct, err = structpb.NewStruct(mdMap)
			if err != nil {
				return nil, fmt.Errorf("converting ledger metadata to protobuf Struct for '%s': %w", ledgerInfo.Name, err)
			}
		}

		ledgerResp := &service.CreateLedgerResponse{
			Id:        ledgerInfo.ID,
			Name:      ledgerInfo.Name,
			Config:    cfg,
			Driver:    ledgerInfo.Driver,
			Metadata:  mdStruct,
			CreatedAt: timestamppb.New(ledgerInfo.CreatedAt.Time),
		}
		if ledgerInfo.SnapshotThreshold > 0 {
			ledgerResp.SnapshotThreshold = ledgerInfo.SnapshotThreshold
		}

		ledgersList = append(ledgersList, ledgerResp)
	}

	return &service.GetAllLedgersResponse{
		Ledgers: ledgersList,
	}, nil
}

func (impl *SystemServiceServerImpl) GetLedgerInfo(ctx context.Context, req *service.GetLedgerByNameRequest) (*service.GetLedgerByNameResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("GetLedgerInfo request received")

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerInfo, err := impl.systemNode.GetLedgerInfo(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", req.Name, err)
	}

	// Convert json.RawMessage to map[string]interface{} for protobuf conversion
	var configMap map[string]interface{}
	if len(ledgerInfo.Config) > 0 {
		if err := json.Unmarshal(ledgerInfo.Config, &configMap); err != nil {
			return nil, fmt.Errorf("unmarshaling ledger config: %w", err)
		}
	}

	cfg, err := structpb.NewStruct(configMap)
	if err != nil {
		return nil, fmt.Errorf("converting ledger config to protobuf Struct: %w", err)
	}

	// Convert metadata
	var mdStruct *structpb.Struct
	if len(ledgerInfo.Metadata) > 0 {
		mdMap := make(map[string]interface{})
		for k, v := range ledgerInfo.Metadata {
			mdMap[k] = v
		}
		mdStruct, err = structpb.NewStruct(mdMap)
		if err != nil {
			return nil, fmt.Errorf("converting ledger metadata to protobuf Struct: %w", err)
		}
	}

	resp := &service.GetLedgerByNameResponse{
		Id:        ledgerInfo.ID,
		Name:      ledgerInfo.Name,
		Config:    cfg,
		Driver:    ledgerInfo.Driver,
		Metadata:  mdStruct,
		CreatedAt: timestamppb.New(ledgerInfo.CreatedAt.Time),
	}
	if ledgerInfo.SnapshotThreshold > 0 {
		resp.SnapshotThreshold = ledgerInfo.SnapshotThreshold
	}

	return resp, nil
}

func (impl *SystemServiceServerImpl) ResolveLedgerLeader(ctx context.Context, req *service.ResolveLedgerLeaderRequest) (*service.ResolveLedgerLeaderResponse, error) {
	impl.logger.WithFields(map[string]any{"ledger_name": req.LedgerName}).Debugf("ResolveLedgerLeader request received")

	if req.LedgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, req.LedgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", req.LedgerName, err)
	}

	return &service.ResolveLedgerLeaderResponse{
		LeaderId: ledgerNode.GetLeader(),
	}, nil
}

func RegisterSystemService(server *grpc.Server, systemServiceServer service.SystemServiceServer) {
	service.RegisterSystemServiceServer(server, systemServiceServer)
}
