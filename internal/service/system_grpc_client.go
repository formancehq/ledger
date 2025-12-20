package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
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

func (g *SystemGRPCClient) CreateLedger(ctx context.Context, name, driver string, config map[string]interface{}, md map[string]string, snapshotThreshold *uint64) (*ledger.LedgerInfo, error) {
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

	// Convert protobuf Struct to json.RawMessage
	configJSON, err := json.Marshal(ledgerResp.Config.AsMap())
	if err != nil {
		return nil, err
	}

	// Convert metadata
	var ledgerMetadata metadata.Metadata
	if ledgerResp.Metadata != nil {
		mdMap := ledgerResp.Metadata.AsMap()
		ledgerMetadata = make(map[string]string)
		for k, v := range mdMap {
			if str, ok := v.(string); ok {
				ledgerMetadata[k] = str
			}
		}
	}

	result := &ledger.LedgerInfo{
		ID:        ledgerResp.Id,
		Name:      ledgerResp.Name,
		Driver:    ledgerResp.Driver,
		Config:    configJSON,
		Metadata:  ledgerMetadata,
		CreatedAt: time.New(ledgerResp.CreatedAt.AsTime()),
	}
	if ledgerResp.SnapshotThreshold > 0 {
		result.SnapshotThreshold = ledgerResp.SnapshotThreshold
	}
	return result, nil
}

func (g *SystemGRPCClient) DeleteLedger(ctx context.Context, name string) error {
	_, err := g.client.DeleteLedger(ctx, &DeleteLedgerRequest{Name: name})
	return err
}

func (g *SystemGRPCClient) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	resp, err := g.client.ResolveLedger(ctx, &ResolveLedgerRequest{LedgerName: ledgerName})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", 0, ledger.NewNotFoundError("%s", err)
		}
		return "", 0, err
	}
	return resp.LedgerName, resp.LedgerId, nil
}

func (g *SystemGRPCClient) GetAllLedgersInfo(ctx context.Context) map[string]ledger.LedgerInfo {
	resp, err := g.client.GetAllLedgersInfo(ctx, &GetAllLedgersRequest{})
	if err != nil {
		// Return empty map on error - this is a limitation of the interface
		// In practice, this should not happen as GetAllLedgersInfo is typically called locally
		return make(map[string]ledger.LedgerInfo)
	}

	// Convert []*CreateLedgerResponse to map[string]ledger.LedgerInfo
	result := make(map[string]ledger.LedgerInfo, len(resp.Ledgers))
	for _, ledgerResp := range resp.Ledgers {
		// Convert protobuf Struct to json.RawMessage
		configJSON, err := json.Marshal(ledgerResp.Config.AsMap())
		if err != nil {
			// Skip this ledger if config conversion fails
			continue
		}

		// Convert metadata
		var ledgerMetadata metadata.Metadata
		if ledgerResp.Metadata != nil {
			mdMap := ledgerResp.Metadata.AsMap()
			ledgerMetadata = make(map[string]string)
			for k, v := range mdMap {
				if str, ok := v.(string); ok {
					ledgerMetadata[k] = str
				}
			}
		}

		ledgerInfo := ledger.LedgerInfo{
			ID:        ledgerResp.Id,
			Name:      ledgerResp.Name,
			Driver:    ledgerResp.Driver,
			Config:    configJSON,
			Metadata:  ledgerMetadata,
			CreatedAt: time.New(ledgerResp.CreatedAt.AsTime()),
		}
		if ledgerResp.SnapshotThreshold > 0 {
			ledgerInfo.SnapshotThreshold = ledgerResp.SnapshotThreshold
		}

		result[ledgerInfo.Name] = ledgerInfo
	}

	return result
}

func (g *SystemGRPCClient) GetLedgerInfo(ctx context.Context, name string) (*ledger.LedgerInfo, error) {
	resp, err := g.client.GetLedgerInfo(ctx, &GetLedgerByNameRequest{Name: name})
	if err != nil {
		return nil, err
	}

	// Convert protobuf Struct to json.RawMessage
	configJSON, err := json.Marshal(resp.Config.AsMap())
	if err != nil {
		return nil, fmt.Errorf("marshaling ledger config: %w", err)
	}

	// Convert metadata
	var ledgerMetadata metadata.Metadata
	if resp.Metadata != nil {
		mdMap := resp.Metadata.AsMap()
		ledgerMetadata = make(map[string]string)
		for k, v := range mdMap {
			if str, ok := v.(string); ok {
				ledgerMetadata[k] = str
			}
		}
	}

	ledgerInfo := ledger.LedgerInfo{
		ID:        resp.Id,
		Name:      resp.Name,
		Driver:    resp.Driver,
		Config:    configJSON,
		Metadata:  ledgerMetadata,
		CreatedAt: time.New(resp.CreatedAt.AsTime()),
	}
	if resp.SnapshotThreshold > 0 {
		ledgerInfo.SnapshotThreshold = resp.SnapshotThreshold
	}

	return &ledgerInfo, nil
}

var _ System = (*SystemGRPCClient)(nil)

func NewGrpcSystemClient(client SystemServiceClient) *SystemGRPCClient {
	return &SystemGRPCClient{client}
}
