package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type SystemGRPCClient struct {
	client SystemServiceClient
}

func (g *SystemGRPCClient) ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error) {
	ret, err := g.client.ResolveLedgerLeader(ctx, &ResolveLedgerLeaderRequest{LedgerName: ledgerName})
	if err != nil {
		return 0, err
	}
	return ret.LeaderId, nil
}

func (g *SystemGRPCClient) Snapshot(ctx context.Context) error {
	_, err := g.client.Snapshot(ctx, &SnapshotRequest{})
	return err
}

func (g *SystemGRPCClient) CreateLedger(ctx context.Context, name, driver string, config map[string]interface{}, md map[string]string, snapshotThreshold *uint64) (*ledgerpb.LedgerInfo, error) {
	cfg, err := structpb.NewStruct(config)
	if err != nil {
		return nil, err
	}

	var mdStruct *structpb.Struct
	if len(md) > 0 {
		// Convert map[string]string to map[string]interface{}
		mdMap := make(map[string]interface{})
		for k, v := range md {
			mdMap[k] = v
		}
		mdStruct, err = structpb.NewStruct(mdMap)
		if err != nil {
			return nil, err
		}
	}

	req := &CreateLedgerRequest{
		Name:     name,
		Driver:   driver,
		Config:   cfg,
		Metadata: mdStruct,
	}
	if snapshotThreshold != nil && *snapshotThreshold > 0 {
		req.SnapshotThreshold = *snapshotThreshold
	}
	ledgerResp, err := g.client.CreateLedger(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert CreateLedgerResponse to LedgerInfo
	var metadata map[string]string
	if ledgerResp.Metadata != nil {
		metadata = ledgerpb.StructToMetadata(ledgerResp.Metadata)
	}
	return &ledgerpb.LedgerInfo{
		Id:                ledgerResp.Id,
		Name:              ledgerResp.Name,
		Driver:            ledgerResp.Driver,
		Config:            ledgerResp.Config,
		Metadata:          metadata,
		CreatedAt:         ledgerResp.CreatedAt,
		SnapshotThreshold: ledgerResp.SnapshotThreshold,
	}, nil
}

func (g *SystemGRPCClient) DeleteLedger(ctx context.Context, name string) error {
	_, err := g.client.DeleteLedger(ctx, &DeleteLedgerRequest{Name: name})
	return err
}

func (g *SystemGRPCClient) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	resp, err := g.client.ResolveLedger(ctx, &ResolveLedgerRequest{LedgerName: ledgerName})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", 0, ledgerpb.NewNotFoundError("%s", err)
		}
		return "", 0, err
	}
	return resp.LedgerName, resp.LedgerId, nil
}

func (g *SystemGRPCClient) GetAllLedgersInfo(ctx context.Context) map[string]*ledgerpb.LedgerInfo {
	resp, err := g.client.GetAllLedgersInfo(ctx, &GetAllLedgersRequest{})
	if err != nil {
		// Return empty map on error - this is a limitation of the interface
		// In practice, this should not happen as GetAllLedgersInfo is typically called locally
		return make(map[string]*ledgerpb.LedgerInfo)
	}

	// Convert []*CreateLedgerResponse to map[string]*ledgerpb.LedgerInfo
	result := make(map[string]*ledgerpb.LedgerInfo, len(resp.Ledgers))
	for _, ledgerResp := range resp.Ledgers {
		var metadata map[string]string
		if ledgerResp.Metadata != nil {
			metadata = ledgerpb.StructToMetadata(ledgerResp.Metadata)
		}
		result[ledgerResp.Name] = &ledgerpb.LedgerInfo{
			Id:                ledgerResp.Id,
			Name:              ledgerResp.Name,
			Driver:            ledgerResp.Driver,
			Config:            ledgerResp.Config,
			Metadata:          metadata,
			CreatedAt:         ledgerResp.CreatedAt,
			SnapshotThreshold: ledgerResp.SnapshotThreshold,
		}
	}

	return result
}

func (g *SystemGRPCClient) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	resp, err := g.client.GetLedgerInfo(ctx, &GetLedgerByNameRequest{Name: name})
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if resp.Metadata != nil {
		metadata = ledgerpb.StructToMetadata(resp.Metadata)
	}
	return &ledgerpb.LedgerInfo{
		Id:                resp.Id,
		Name:              resp.Name,
		Driver:            resp.Driver,
		Config:            resp.Config,
		Metadata:          metadata,
		CreatedAt:         resp.CreatedAt,
		SnapshotThreshold: resp.SnapshotThreshold,
	}, nil
}

var _ System = (*SystemGRPCClient)(nil)

func NewGrpcSystemClient(client SystemServiceClient) *SystemGRPCClient {
	return &SystemGRPCClient{client}
}
