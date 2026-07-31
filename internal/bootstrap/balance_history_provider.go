package bootstrap

import (
	"context"
	"fmt"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

// balanceHistoryVolumeViewProvider keeps the opt-in rollout fail closed at the
// application boundary.
// Gating here prevents a previously READY local store from serving a stale PIT
// view while the builder and verifier are not enabled.
type balanceHistoryVolumeViewProvider struct {
	enabled        bool
	allowedLedgers map[string]struct{}
	builder        *appbalancehistory.Builder
	local          *ctrl.LocalVolumeViewProvider
}

func newBalanceHistoryVolumeViewProvider(
	store *balancehistorystore.Store,
	builder *appbalancehistory.Builder,
	config Config,
) ctrl.VolumeViewProvider {
	allowedLedgers := make(map[string]struct{}, len(config.BalanceHistoryConfig.Ledgers))
	for _, ledger := range config.BalanceHistoryConfig.Ledgers {
		allowedLedgers[ledger] = struct{}{}
	}

	return &balanceHistoryVolumeViewProvider{
		enabled:        config.BalanceHistoryConfig.Enabled,
		allowedLedgers: allowedLedgers,
		builder:        builder,
		local:          ctrl.NewLocalVolumeViewProvider(store),
	}
}

func (p *balanceHistoryVolumeViewProvider) Open(
	ctx context.Context,
	ledgerName string,
	ledgerID uint32,
	selector ctrl.PointInTimeSelector,
	minLogSequence uint64,
) (*ctrl.HistoricalVolumeView, error) {
	if p == nil || !p.enabled {
		return nil, &balancehistorystore.ErrSourceMissing{Detail: "balance history projection is not enabled by configuration"}
	}
	if len(p.allowedLedgers) > 0 {
		if _, allowed := p.allowedLedgers[ledgerName]; !allowed {
			return nil, &balancehistorystore.ErrSourceMissing{Detail: fmt.Sprintf(
				"balance history reads are not enabled for ledger %q",
				ledgerName,
			)}
		}
	}
	if p.builder == nil || !p.builder.Ready() {
		// Readiness is deliberately process-local. A persisted complete manifest
		// cannot prove that this process has reconciled it with the current
		// authoritative source after startup or while shutting down.
		return nil, &balancehistorystore.ErrBuilding{}
	}

	return p.local.Open(ctx, ledgerName, ledgerID, selector, minLogSequence)
}
