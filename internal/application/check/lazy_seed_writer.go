package check

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	domainreplay "github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

var _ domainreplay.Writer = (*lazyTxSeedWriter)(nil)

// lazyTxSeedWriter wraps a replayStore for the archived-replay pass and seeds a
// transaction's baseline state into the replay the first time a post-archive
// delta touches it, rather than bulk-loading every baseline transaction up
// front. The delta (metadata set/delete, revert marker) then merges onto the
// full pre-archive state whose create log has been purged. A post-archive
// CreateTransaction is a fresh transaction with no baseline, so it is not
// intercepted. Only transactions actually touched are materialized, keeping the
// replay store O(touched) instead of O(all txs ever); untouched transactions
// fall back to the baseline in compareTransactions.
type lazyTxSeedWriter struct {
	*replayStore

	// lookup returns the baseline transaction state for a canonical key, or
	// pebble.ErrNotFound when the key has no pre-archive state.
	lookup func(canonicalKey []byte) (*commonpb.TransactionState, error)
	seeded map[string]struct{}
}

func newLazyTxSeedWriter(replay *replayStore, lookup func(canonicalKey []byte) (*commonpb.TransactionState, error)) *lazyTxSeedWriter {
	return &lazyTxSeedWriter{
		replayStore: replay,
		lookup:      lookup,
		seeded:      make(map[string]struct{}),
	}
}

// seedIfNeeded seeds the baseline state for a key the first time it is touched.
// The key is recorded whether or not a baseline state exists, so the lookup runs
// at most once per key.
func (w *lazyTxSeedWriter) seedIfNeeded(canonicalKey []byte) error {
	sk := string(canonicalKey)
	if _, ok := w.seeded[sk]; ok {
		return nil
	}

	w.seeded[sk] = struct{}{}

	state, err := w.lookup(canonicalKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lazy-seeding transaction from baseline: %w", err)
	}

	return w.SeedTransaction(canonicalKey, state)
}

func (w *lazyTxSeedWriter) SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error {
	if err := w.seedIfNeeded(canonicalKey); err != nil {
		return err
	}

	return w.replayStore.SaveTxMetadata(canonicalKey, metadata)
}

func (w *lazyTxSeedWriter) DeleteTxMetadata(canonicalKey []byte, key string) error {
	if err := w.seedIfNeeded(canonicalKey); err != nil {
		return err
	}

	return w.replayStore.DeleteTxMetadata(canonicalKey, key)
}

func (w *lazyTxSeedWriter) SetRevertedBy(canonicalKey []byte, revertTxID uint64, revertedAt *commonpb.Timestamp) error {
	if err := w.seedIfNeeded(canonicalKey); err != nil {
		return err
	}

	return w.replayStore.SetRevertedBy(canonicalKey, revertTxID, revertedAt)
}
