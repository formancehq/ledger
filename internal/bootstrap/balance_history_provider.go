package bootstrap

import (
	"context"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

// balanceHistoryVolumeViewProvider gates reads on process-local reconciliation.
// Per-ledger enablement is read from the audit-derived manifest by local.Open.
type balanceHistoryVolumeViewProvider struct {
	builder *appbalancehistory.Builder
	local   *ctrl.LocalVolumeViewProvider
}

func newBalanceHistoryVolumeViewProvider(store *balancehistorystore.Store, builder *appbalancehistory.Builder) ctrl.VolumeViewProvider {
	return &balanceHistoryVolumeViewProvider{
		builder: builder,
		local:   ctrl.NewLocalVolumeViewProvider(store),
	}
}

func (p *balanceHistoryVolumeViewProvider) Open(
	ctx context.Context,
	ledgerName string,
	selector ctrl.HistoricalBalanceSelector,
	minLogSequence uint64,
) (*ctrl.HistoricalVolumeView, error) {
	if p == nil || p.builder == nil || !p.builder.Ready() {
		return nil, &balancehistorystore.ErrBuilding{}
	}

	return p.local.Open(ctx, ledgerName, selector, minLogSequence)
}

func (p *balanceHistoryVolumeViewProvider) Status(ctx context.Context, ledgerName string) (*servicepb.GetHistoricalBalancesStatusResponse, error) {
	if p == nil || p.local == nil {
		return &servicepb.GetHistoricalBalancesStatusResponse{
			Ledger: ledgerName,
			State:  servicepb.GetHistoricalBalancesStatusResponse_STATE_ERROR,
			Error:  "historical-balance provider is unavailable",
		}, nil
	}
	status, err := p.local.Status(ctx, ledgerName)
	if err != nil {
		return nil, err
	}
	if status.GetState() == servicepb.GetHistoricalBalancesStatusResponse_STATE_READY && (p.builder == nil || !p.builder.Ready()) {
		status.State = servicepb.GetHistoricalBalancesStatusResponse_STATE_BUILDING
	}

	return status, nil
}
