package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// metadataBatchProposer implements state.MetadataBatchProposer by
// running each proposal through the preload runner. The runner
// resolves the canonical keys via cache → bloom → Pebble, attaches
// them to cmd.Preload so the FSM cache is populated before
// applyMetadataConversionBatch runs its compare-and-set, and sets
// PredictedIndex as a backstop against tracker drift.
//
// This adapter lives in bootstrap (not state) because state cannot
// import preload: preload depends on state for ApplyResult, which
// would create a cycle. The adapter is a thin glue layer.
type metadataBatchProposer struct {
	builder  *plan.Builder
	proposer plan.Proposer
}

func newMetadataBatchProposer(builder *plan.Builder, proposer plan.Proposer) *metadataBatchProposer {
	return &metadataBatchProposer{
		builder:  builder,
		proposer: proposer,
	}
}

func (m *metadataBatchProposer) Propose(
	ctx context.Context,
	cmd *raftcmdpb.Proposal,
	canonicalKeys [][]byte,
	target commonpb.TargetType,
) error {
	needs := plan.NewNeeds()

	// `applyMetadataConversionBatch` and `applyMetadataConversionCompletion`
	// both read `fsm.Registry.Ledgers.Get(ledgerKey)` (staleness check on
	// the schema field, and saving Status=COMPLETE for the completion
	// path). Declare the ledger so the preload populates the cache with
	// the fresh Pebble value at propose time.
	for _, tu := range cmd.GetTechnicalUpdates() {
		switch kind := tu.GetKind().(type) {
		case *raftcmdpb.TechnicalUpdate_MetadataBatch:
			if name := kind.MetadataBatch.GetLedger(); name != "" {
				needs.Ledgers[domain.LedgerKey{Name: name}] = struct{}{}
			}
		case *raftcmdpb.TechnicalUpdate_MetadataCompletion:
			if name := kind.MetadataCompletion.GetLedger(); name != "" {
				needs.Ledgers[domain.LedgerKey{Name: name}] = struct{}{}
			}
		}
	}

	// `applyConvertEntry` reads the cache for each canonical key via
	// `getMetadataCacheEntry`. Declare them per target type.
	for _, ck := range canonicalKeys {
		switch target {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			var mk domain.MetadataKey
			if err := mk.Unmarshal(ck); err != nil {
				return fmt.Errorf("unmarshaling account metadata canonical key: %w", err)
			}

			needs.Metadata[mk] = struct{}{}
		case commonpb.TargetType_TARGET_TYPE_LEDGER:
			var lmk domain.LedgerMetadataKey
			if err := lmk.Unmarshal(ck); err != nil {
				return fmt.Errorf("unmarshaling ledger metadata canonical key: %w", err)
			}

			needs.LedgerMetadata[lmk] = struct{}{}
		default:
			// Defensive: callers should never ship canonical keys for
			// targets we don't know how to preload. Transaction
			// metadata uses read-time enforcement and never reaches
			// this path (see metadata_converter.convert's early return
			// for TRANSACTION). Returning an error here surfaces a
			// future regression loudly instead of mis-parsing the
			// canonical bytes as account-metadata keys.
			return fmt.Errorf("unsupported target type for metadata preload: %v", target)
		}
	}

	// One WriteOperation per TU in the proposal — each TU declared its
	// reads above when we walked the TechnicalUpdates list. The proposer
	// only ever ships one TU per cmd today (MetadataBatch xor
	// MetadataCompletion), but the per-TU mapping keeps the path safe
	// for future multi-TU batches.
	tus := cmd.GetTechnicalUpdates()
	operations := make([]plan.WriteOperation, 0, len(tus))
	for i := range tus {
		operations = append(operations, plan.WriteOperation{
			Needs: needs,
			SetCoverage: func(bits []byte) {
				cmd.GetTechnicalUpdates()[i].CoverageBits = bits
			},
		})
	}

	build, err := m.builder.Build(operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		return fmt.Errorf("building preloads for metadata batch: %w", err)
	}

	result, err := m.builder.Run(
		ctx, cmd, build,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return c.MarshalVT() },
		m.proposer,
	)
	if err != nil {
		return err
	}

	// Converter is single-goroutine per (ledger, key); no loader
	// dedup is needed across concurrent proposals.
	result.Guard.ReleaseLoaders()

	// The converter passes a context derived from its stop channel. On
	// shutdown the Raft node stops and these futures would never
	// resolve; cancelling ctx unblocks both waits and lets the retry
	// loop observe stop and exit cleanly within OnStop's deadline.
	if _, err := result.Proposal.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for raft acceptance: %w", err)
	}

	res, err := result.FSMFuture.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for FSM apply: %w", err)
	}

	if res.Error != nil {
		if errors.Is(res.Error, domain.ErrStaleProposal) {
			// Surface the stale-proposal error so the converter's
			// retry loop picks a fresh predicted index.
			return res.Error
		}

		return fmt.Errorf("applying metadata batch: %w", res.Error)
	}

	return nil
}
