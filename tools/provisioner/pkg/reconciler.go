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

	if err := r.handleExporters(ctx, cfg, state); err != nil {
		return err
	}

	if err := r.handleLedgers(ctx, cfg, state); err != nil {
		return err
	}

	return nil
}

func (r Reconciler) handleExporters(ctx context.Context, cfg Config, state *State) error {
	for exporterName, exporterConfig := range cfg.Exporters {
		existingExporterState, ok := state.Exporters[exporterName]
		if ok {
			if !cmp.Equal(exporterConfig, existingExporterState.Config, cmpopts.EquateEmpty()) {
				fmt.Printf("Config for exporter %s has changed, deleting exporter...\r\n", exporterName)
				_, err := r.ledgerClient.Ledger.V2.DeleteExporter(ctx, operations.V2DeleteExporterRequest{
					ExporterID: existingExporterState.ID,
				})
				if err != nil {
					return fmt.Errorf("failed to delete exporter %s: %w", exporterName, err)
				}
			} else {
				fmt.Printf("Config for exporter %s is up to date\r\n", exporterName)
				continue
			}
		}

		fmt.Printf("Creating exporter %s...\r\n", exporterName)
		ret, err := r.ledgerClient.Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
			Driver: exporterConfig.Driver,
			Config: exporterConfig.Config,
		})
		if err != nil {
			return fmt.Errorf("failed to create exporter %s: %w", exporterName, err)
		}
		fmt.Printf("Exporter %s created.\r\n", exporterName)

		state.Exporters[exporterName] = &ExporterState{
			ID:     ret.V2CreateExporterResponse.Data.ID,
			Config: exporterConfig,
		}
	}

	if state.Exporters != nil {
		for exporterName, exporterState := range state.Exporters {
			_, configExists := cfg.Exporters[exporterName]
			if !configExists {
				fmt.Printf("Exporter %s removed\r\n", exporterName)
				_, err := r.ledgerClient.Ledger.V2.DeleteExporter(ctx, operations.V2DeleteExporterRequest{
					ExporterID: exporterState.ID,
				})
				if err != nil {
					return fmt.Errorf("failed to delete exporter %s: %w", exporterName, err)
				}

				state.removeExporter(exporterName)
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
				Exporters: map[string]string{},
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

		for _, exporter := range ledgerConfig.Exporters {
			if slices.Contains(Keys(ledgerState.Exporters), exporter) {
				continue
			}

			fmt.Printf(
				"Detect new exporter binding for ledger %s and exporter %s, creating a new pipeline...\r\n",
				ledgerName,
				exporter,
			)

			ret, err := r.ledgerClient.Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
				Ledger: ledgerName,
				V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
					ExporterID: state.Exporters[exporter].ID,
				},
			})
			if err != nil {
				return fmt.Errorf("failed to create pipeline for ledger %s and exporter %s: %w", ledgerName, exporter, err)
			}
			fmt.Printf("Pipeline %s created.\r\n", ret.V2CreatePipelineResponse.Data.ID)

			ledgerState.Exporters[exporter] = ret.V2CreatePipelineResponse.Data.ID
		}

		for _, exporter := range Keys(ledgerState.Exporters) {
			if slices.Contains(ledgerConfig.Exporters, exporter) {
				continue
			}

			fmt.Printf(
				"Detect removed exporter binding for ledger %s and exporter %s, deleting pipeline %s...\r\n",
				ledgerName,
				exporter,
				ledgerState.Exporters[exporter],
			)

			_, err := r.ledgerClient.Ledger.V2.DeletePipeline(ctx, operations.V2DeletePipelineRequest{
				Ledger:     ledgerName,
				PipelineID: ledgerState.Exporters[exporter],
			})
			if err != nil {
				return fmt.Errorf("failed to delete pipeline for ledger %s and exporter %s: %w", ledgerName, exporter, err)
			}

			ledgerState.removeExporterBinding(exporter)
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
