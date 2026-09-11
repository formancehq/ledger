package indexbuilder

import (
	"errors"
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

type ledgerHistoryState byte

var errHistoryReplayInvariant = errors.New("ledger history replay invariant")

func historyReplayInvariantf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errHistoryReplayInvariant, fmt.Sprintf(format, args...))
}

const (
	ledgerHistoryUnknown ledgerHistoryState = iota
	ledgerHistoryEmpty
	ledgerHistoryNonEmpty
)

type ledgerHistoryMutation struct {
	state   ledgerHistoryState
	deleted bool
}

func (b *Builder) historyStateFor(ledger string) (ledgerHistoryState, bool) {
	if mutation, ok := b.historyOverlay[ledger]; ok {
		if mutation.deleted {
			return ledgerHistoryUnknown, false
		}

		return mutation.state, true
	}

	state, ok := b.ledgerHistory[ledger]

	return state, ok
}

func (b *Builder) loadLedgerHistory(reader dal.PebbleReader) error {
	entries, err := readstore.ReadAllLedgerHistoryStatesFrom(reader)
	if err != nil {
		if errors.Is(err, readstore.ErrLedgerHistoryCorrupt) {
			return historyReplayInvariantf("%v", err)
		}

		return err
	}

	b.ledgerHistory = make(map[string]ledgerHistoryState, len(entries))
	for _, entry := range entries {
		state := ledgerHistoryState(entry.State)
		if state != ledgerHistoryEmpty && state != ledgerHistoryNonEmpty {
			return historyReplayInvariantf("corrupt ledger history state for %q: unknown value %d", entry.LedgerName, entry.State)
		}
		if _, duplicate := b.ledgerHistory[entry.LedgerName]; duplicate {
			return historyReplayInvariantf("corrupt ledger history state: duplicate ledger %q", entry.LedgerName)
		}

		b.ledgerHistory[entry.LedgerName] = state
	}

	return nil
}

func (b *Builder) observeCreatedLedger(ledger string) error {
	if ledger == "" {
		return historyReplayInvariantf("CreatedLedger has an empty ledger name")
	}
	if _, exists := b.historyStateFor(ledger); exists {
		return historyReplayInvariantf("CreatedLedger for %q encountered while a history state already exists", ledger)
	}
	if b.wb == nil || b.wb.Batch() == nil {
		return historyReplayInvariantf("CreatedLedger for %q encountered without an active readstore batch", ledger)
	}
	if err := b.wb.WriteLedgerHistoryState(b.kb, ledger, byte(ledgerHistoryEmpty)); err != nil {
		return fmt.Errorf("persisting EMPTY ledger history state for %q: %w", ledger, err)
	}

	b.historyOverlay[ledger] = ledgerHistoryMutation{state: ledgerHistoryEmpty}
	if _, exists := b.indexConfig[ledger]; !exists {
		b.indexConfig[ledger] = newLedgerIndexConfig()
		b.recordFoldRollback(func() { delete(b.indexConfig, ledger) })
	}

	return nil
}

func (b *Builder) observeLedgerPayload(ledger string, payload *commonpb.LedgerLogPayload) error {
	category := commonpb.LedgerLogCategoryOf(payload)
	if category == commonpb.LedgerLogCategory_LEDGER_LOG_CATEGORY_UNSPECIFIED {
		return historyReplayInvariantf("ledger %q emitted an unclassified ledger log payload %T", ledger, payload.GetPayload())
	}

	state, exists := b.historyStateFor(ledger)
	if !exists {
		return historyReplayInvariantf("ledger log for %q has no EMPTY/NON_EMPTY history state", ledger)
	}
	if state != ledgerHistoryEmpty && state != ledgerHistoryNonEmpty {
		return historyReplayInvariantf("ledger %q has invalid history state %d", ledger, state)
	}
	if b.wb == nil || b.wb.Batch() == nil {
		return historyReplayInvariantf("ledger log for %q encountered without an active readstore batch", ledger)
	}
	if category != commonpb.LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY || state == ledgerHistoryNonEmpty {
		return nil
	}

	if err := b.wb.WriteLedgerHistoryState(b.kb, ledger, byte(ledgerHistoryNonEmpty)); err != nil {
		return fmt.Errorf("persisting NON_EMPTY ledger history state for %q: %w", ledger, err)
	}
	b.historyOverlay[ledger] = ledgerHistoryMutation{state: ledgerHistoryNonEmpty}

	return nil
}

func (b *Builder) observeDeletedLedger(ledger string) error {
	if b.wb == nil || b.wb.Batch() == nil {
		return historyReplayInvariantf("DeleteLedger for %q encountered without an active readstore batch", ledger)
	}
	if _, exists := b.historyStateFor(ledger); !exists {
		// Ledger deletion is idempotent at the API/FSM boundary, so repeated
		// DeleteLedger logs are valid. The first one already removed the durable
		// tracker and all generation-scoped builder state.
		return nil
	}
	if err := b.wb.DeleteLedgerHistoryState(b.kb, ledger); err != nil {
		return fmt.Errorf("deleting ledger history state for %q: %w", ledger, err)
	}
	b.historyOverlay[ledger] = ledgerHistoryMutation{deleted: true}
	b.dropLedgerBuilderState(ledger)

	return nil
}

func (b *Builder) dropLedgerBuilderState(ledger string) {
	priorConfig, hadConfig := b.indexConfig[ledger]
	priorVersions, hadVersions := b.indexVersions[ledger]
	priorUnresolved, hadUnresolved := b.unresolvedIndexes[ledger]
	priorBackfills := slices.Clone(b.backfillTasks)
	priorRewrites := slices.Clone(b.schemaRewriteTasks)
	priorNextBackfill := b.nextBackfillIdx
	b.recordFoldRollback(func() {
		if hadConfig {
			b.indexConfig[ledger] = priorConfig
		} else {
			delete(b.indexConfig, ledger)
		}
		if hadVersions {
			b.indexVersions[ledger] = priorVersions
		} else {
			delete(b.indexVersions, ledger)
		}
		if hadUnresolved {
			b.unresolvedIndexes[ledger] = priorUnresolved
		} else {
			delete(b.unresolvedIndexes, ledger)
		}
		b.backfillTasks = priorBackfills
		b.schemaRewriteTasks = priorRewrites
		b.nextBackfillIdx = priorNextBackfill
	})

	delete(b.indexConfig, ledger)
	delete(b.indexVersions, ledger)
	delete(b.unresolvedIndexes, ledger)
	b.backfillTasks = slices.DeleteFunc(b.backfillTasks, func(task *backfillTask) bool {
		return task.ledger == ledger
	})
	b.schemaRewriteTasks = slices.DeleteFunc(b.schemaRewriteTasks, func(task *schemaRewriteTask) bool {
		return task.ledger == ledger
	})
	if b.nextBackfillIdx >= len(b.backfillTasks) {
		b.nextBackfillIdx = 0
	}
}
