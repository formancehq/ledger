package provisionner

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type Reconciler struct {
	store        Store
	ledgerClient *client.Formance
}

func (r Reconciler) Reconcile(ctx context.Context, cfg Config) error {

	if cfg.Ledgers == nil {
		cfg.Ledgers = map[string]LedgerConfig{}
	}

	state, err := r.store.Read(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.store.Update(context.WithoutCancel(ctx), *state); err != nil {
			fmt.Printf("Failed to update state: %v\r\n", err)
		}
	}()

	if state.Ledgers == nil {
		state.Ledgers = map[string]LedgerConfig{}
	}

	for ledgerName, ledgerConfig := range cfg.Ledgers {

		existingConfig, ok := state.Ledgers[ledgerName]
		if ok {
			if !cmp.Equal(ledgerConfig, existingConfig, cmpopts.EquateEmpty()) {
				fmt.Printf("Config for ledger %s was updated but it is not supported at this time\r\n", ledgerName)
			} else {
				fmt.Printf("Config for ledger %s is up to date\r\n", ledgerName)
			}
			continue
		}

		fmt.Printf("Creating ledger %s...\r\n", ledgerName)
		if _, err := r.ledgerClient.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: ledgerName,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket:   pointer.For(ledgerConfig.Bucket),
				Features: ledgerConfig.Features,
				Metadata: ledgerConfig.Metadata,
			},
		}); err != nil {
			return fmt.Errorf("failed to create ledger %s: %w", ledgerName, err)
		}
		fmt.Printf("Ledger %s created...\r\n", ledgerName)

		state.Ledgers[ledgerName] = ledgerConfig
	}

	if state.Ledgers != nil {
		for ledgerName := range state.Ledgers {
			_, configExists := cfg.Ledgers[ledgerName]
			if !configExists {
				fmt.Printf("Ledger %s was removed from config but cannot be removed on the ledger\r\n", ledgerName)
			}
		}
	}

	return nil
}

func NewReconciler(store Store, ledgerClient *client.Formance) *Reconciler {
	return &Reconciler{
		store:        store,
		ledgerClient: ledgerClient,
	}
}
