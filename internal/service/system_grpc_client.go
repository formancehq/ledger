package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SystemGRPCClient struct {
	client systempb.SystemServiceClient
}

func (g *SystemGRPCClient) ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error) {
	ret, err := g.client.ResolveLedgerLeader(ctx, &systempb.ResolveLedgerLeaderRequest{LedgerName: ledgerName})
	if err != nil {
		return 0, err
	}
	return ret.LeaderId, nil
}

func (g *SystemGRPCClient) CreateLedger(ctx context.Context, request *systempb.CreateLedgerRequest) (*ledgerpb.LedgerInfo, error) {
	return g.client.CreateLedger(ctx, request)
}

func (g *SystemGRPCClient) DeleteLedger(ctx context.Context, name string) error {
	_, err := g.client.DeleteLedger(ctx, &systempb.DeleteLedgerRequest{Name: name})
	return err
}

func (g *SystemGRPCClient) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	resp, err := g.client.ResolveLedger(ctx, &systempb.ResolveLedgerRequest{LedgerName: ledgerName})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", 0, ledgerpb.NewNotFoundError("%s", err)
		}
		return "", 0, err
	}
	return resp.LedgerName, resp.LedgerId, nil
}

func (g *SystemGRPCClient) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	resp, err := g.client.GetAllLedgersInfo(ctx, &systempb.GetAllLedgersRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Ledgers, nil
}

func (g *SystemGRPCClient) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return g.client.GetLedgerInfo(ctx, &systempb.GetLedgerByNameRequest{Name: name})
}

var _ System = (*SystemGRPCClient)(nil)

func NewGrpcSystemClient(client systempb.SystemServiceClient) *SystemGRPCClient {
	return &SystemGRPCClient{client}
}
