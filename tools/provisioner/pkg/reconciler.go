package provisionner

import (
	"context"
	"fmt"
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"slices"
)

type Reconciler struct {
	store        Store
	ledgerClient *client.Formance
}

func (r Reconciler) Reconcile(ctx context.Context, cfg Config) error {
	cfg.setDefaults()

	state, err := r.store.Read(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.store.Update(context.WithoutCancel(ctx), *state); err != nil {
			fmt.Printf("Failed to update state: %v\r\n", err)
		}
	}()
	state.setDefaults()

	if err := r.handleConnectors(ctx, cfg, state); err != nil {
		return err
	}

	if err := r.handleLedgers(ctx, cfg, state); err != nil {
		return err
	}

	return nil
}

func (r Reconciler) handleConnectors(ctx context.Context, cfg Config, state *State) error {
	for connectorName, connectorConfig := range cfg.Connectors {
		existingConnectorState, ok := state.Connectors[connectorName]
		if ok {
			if !cmp.Equal(connectorConfig, existingConnectorState.Config, cmpopts.EquateEmpty()) {
				fmt.Printf("Config for connector %s has changed, deleting connector...\r\n", connectorName)
				_, err := r.ledgerClient.Ledger.V2.DeleteConnector(ctx, operations.V2DeleteConnectorRequest{
					ConnectorID: existingConnectorState.ID,
				})
				if err != nil {
					return fmt.Errorf("failed to delete connector %s: %w", connectorName, err)
				}
			} else {
				fmt.Printf("Config for connector %s is up to date\r\n", connectorName)
				continue
			}
		}

		fmt.Printf("Creating connector %s...\r\n", connectorName)
		ret, err := r.ledgerClient.Ledger.V2.CreateConnector(ctx, components.V2ConnectorConfiguration{
			Driver: connectorConfig.Driver,
			Config: connectorConfig.Config,
		})
		if err != nil {
			return fmt.Errorf("failed to create connector %s: %w", connectorName, err)
		}
		fmt.Printf("Connector %s created.\r\n", connectorName)

		state.Connectors[connectorName] = &ConnectorState{
			ID:     ret.V2CreateConnectorResponse.Data.ID,
			Config: connectorConfig,
		}
	}

	if state.Connectors != nil {
		for connectorName, connectorState := range state.Connectors {
			_, configExists := cfg.Connectors[connectorName]
			if !configExists {
				fmt.Printf("Connector %s removed\r\n", connectorName)
				_, err := r.ledgerClient.Ledger.V2.DeleteConnector(ctx, operations.V2DeleteConnectorRequest{
					ConnectorID: connectorState.ID,
				})
				if err != nil {
					return fmt.Errorf("failed to delete connector %s: %w", connectorName, err)
				}

				state.removeConnector(connectorName)
			}
		}
	}

	return nil
}

func (r Reconciler) handleLedgers(ctx context.Context, cfg Config, state *State) error {
	for ledgerName, ledgerConfig := range cfg.Ledgers {
		ledgerState, ok := state.Ledgers[ledgerName]
		if !ok {
			ledgerState = &LedgerState{
				Connectors: map[string]string{},
			}
			state.Ledgers[ledgerName] = ledgerState

			fmt.Printf("Creating ledger %s...\r\n", ledgerName)
			_, err := r.ledgerClient.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket:   pointer.For(ledgerConfig.Bucket),
					Features: ledgerConfig.Features,
					Metadata: ledgerConfig.Metadata,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create ledger %s: %w", ledgerName, err)
			}
			fmt.Printf("Ledger %s created.\r\n", ledgerName)

			ledgerState.Config = ledgerConfig
		} else {
			if !cmp.Equal(ledgerConfig.LedgerCreateConfig, ledgerState.Config.LedgerCreateConfig, cmpopts.EquateEmpty()) {
				fmt.Printf("Config for ledger %s was updated but it is not supported at this time\r\n", ledgerName)
			} else {
				fmt.Printf("Config for ledger %s is up to date\r\n", ledgerName)
			}
		}

		for _, connector := range ledgerConfig.Connectors {
			if slices.Contains(Keys(ledgerState.Connectors), connector) {
				continue
			}

			fmt.Printf(
				"Detect new connector binding for ledger %s and connector %s, creating a new pipeline...\r\n",
				ledgerName,
				connector,
			)

			ret, err := r.ledgerClient.Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
				Ledger: ledgerName,
				V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
					ConnectorID: state.Connectors[connector].ID,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create pipeline for ledger %s and connector %s: %w", ledgerName, connector, err)
			}
			fmt.Printf("Pipeline %s created.\r\n", ret.V2CreatePipelineResponse.Data.ID)

			ledgerState.Connectors[connector] = ret.V2CreatePipelineResponse.Data.ID
		}

		for _, connector := range Keys(ledgerState.Connectors) {
			if slices.Contains(ledgerConfig.Connectors, connector) {
				continue
			}

			fmt.Printf(
				"Detect removed connector binding for ledger %s and connector %s, deleting pipeline %s...\r\n",
				ledgerName,
				connector,
				ledgerState.Connectors[connector],
			)

			_, err := r.ledgerClient.Ledger.V2.DeletePipeline(ctx, operations.V2DeletePipelineRequest{
				Ledger:     ledgerName,
				PipelineID: ledgerState.Connectors[connector],
			})
			if err != nil {
				return fmt.Errorf("failed to delete pipeline for ledger %s and connector %s: %w", ledgerName, connector, err)
			}

			ledgerState.removeConnectorBinding(connector)
		}
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
