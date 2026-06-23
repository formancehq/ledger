package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// appliedProposalSync maintains a Pebble iterator over the AppliedProposal
// stream and exposes the transient-volume exclusion set for the current
// proposal. Purged volumes are NOT served from here — they live on each
// Log.purged_volumes and are read inline by process_logs.
//
// The sync algorithm is a merge of two sorted streams:
//   - The log iterator advances sequentially (handled by processLogs).
//   - The AppliedProposal iterator runs ahead; for each entry it caches the
//     log_sequence range and the transient_volumes map.
//
// When processLogs encounters a log whose sequence falls within the current
// AppliedProposal's range, the transient set is available. When the log
// sequence exceeds the range, the iterator advances to the next entry.
// Failed proposals leave gaps in the sequence space (no AppliedProposal
// emitted); the iterator transparently skips over them.
type appliedProposalSync struct {
	// Current loaded entry (nil when exhausted or not yet loaded).
	current *proposalpb.AppliedProposal

	// Log sequence range covered by the current entry.
	minLogSeq uint64
	maxLogSeq uint64

	// Sequence of the current entry (for progress persistence).
	currentSeq uint64
	// Last sequence known to be entirely before the next log we care about.
	resumeAfterSeq uint64

	// Cached transient (account, asset) set keyed by ledger to avoid
	// rebuilding for every log in the same proposal.
	cachedLedger    string
	cachedTransient map[domain.AccountAssetKey]struct{}

	cursor    cursor.Cursor[*proposalpb.AppliedProposal]
	exhausted bool
	// iterErr captures the first non-EOF error advance() saw on the
	// cursor (corrupt proto, Pebble iterator failure, etc.). The caller
	// MUST check err() after every batch — a silently-empty transient set
	// would otherwise let the indexer write account->tx mappings for
	// volumes that should have been skipped, producing a wrong index
	// the operator has no signal about.
	iterErr error
}

// newAppliedProposalSync creates an appliedProposalSync that reads entries
// from the given Pebble handle, starting after afterSeq.
func newAppliedProposalSync(ctx context.Context, handle dal.PebbleReader, afterSeq uint64) (*appliedProposalSync, error) {
	var filter *uint64
	if afterSeq > 0 {
		filter = &afterSeq
	}

	cursor, err := query.ReadAppliedProposals(ctx, handle, filter)
	if err != nil {
		return nil, err
	}

	s := &appliedProposalSync{cursor: cursor, resumeAfterSeq: afterSeq}
	// Pre-load the first usable entry.
	s.advance()

	return s, nil
}

// advance moves to the next AppliedProposal entry, skipping entries that did
// not produce any log (they cover no log sequence range). Non-EOF cursor
// errors (corrupt proto, Pebble failure) are stashed on iterErr and the
// sync is marked exhausted so a downstream silent-empty-set does not paper
// over the failure — the caller MUST check err() after each batch.
func (s *appliedProposalSync) advance() {
	for {
		entry, err := s.cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.exhausted = true
				s.current = nil

				return
			}

			// Non-EOF: real iterator/unmarshal failure. Mark the sync
			// exhausted so subsequent transientForLedger calls return
			// nil instead of the last-cached (potentially stale) entry,
			// and surface the error to the caller via err().
			s.iterErr = fmt.Errorf("reading applied proposal: %w", err)
			s.exhausted = true
			s.current = nil

			return
		}

		if entry.GetMaxLogSequence() == 0 {
			// All-idempotent proposal: nothing for the index builder to
			// process. Skip but keep the resume sequence up to date.
			s.currentSeq = entry.GetSequence()
			s.resumeAfterSeq = entry.GetSequence()

			continue
		}

		s.current = entry
		s.currentSeq = entry.GetSequence()
		s.minLogSeq = entry.GetMinLogSequence()
		s.maxLogSeq = entry.GetMaxLogSequence()
		s.cachedLedger = ""
		s.cachedTransient = nil

		return
	}
}

// transientForLedger advances the iterator until the current entry covers
// logSeq, then returns the transient (account, asset) exclusion set for the
// given ledger (nil when there are none). Purged volumes must be merged in
// by the caller from Log.purged_volumes.
//
// Coverage invariant: every successful proposal writes an AppliedProposal
// entry covering its created logs, atomically in the same Pebble batch.
// Failed and all-idempotent proposals produce no logs (failed) or no
// proposal entry (all-idempotent — skipped by advance), so a logSeq we
// are asked about must always fall inside the current proposal's range.
// A mismatch is a should-not-happen and is stashed on iterErr (per
// CLAUDE.md invariant #7); the caller's err() check will surface it
// instead of letting the indexer persist mappings without the proper
// transient exclusions.
func (s *appliedProposalSync) transientForLedger(logSeq uint64, ledger string) map[domain.AccountAssetKey]struct{} {
	if s.iterErr != nil {
		return nil
	}

	if s.exhausted {
		s.iterErr = fmt.Errorf(
			"applied proposal stream exhausted before reaching log %d: "+
				"every successful proposal must emit an AppliedProposal entry covering its created logs",
			logSeq,
		)

		return nil
	}

	s.advanceBefore(logSeq)

	if s.current == nil {
		s.iterErr = fmt.Errorf(
			"applied proposal stream exhausted before reaching log %d: "+
				"every successful proposal must emit an AppliedProposal entry covering its created logs",
			logSeq,
		)

		return nil
	}

	if logSeq < s.minLogSeq {
		s.iterErr = fmt.Errorf(
			"log %d falls in a gap before applied proposal range [%d, %d]: "+
				"a created log must lie inside its proposal's coverage",
			logSeq, s.minLogSeq, s.maxLogSeq,
		)

		return nil
	}

	// logSeq > s.maxLogSeq is structurally unreachable here: advanceBefore
	// only stops when logSeq <= s.maxLogSeq or s.current becomes nil (the
	// branch above).

	if s.cachedLedger == ledger {
		return s.cachedTransient
	}

	s.cachedLedger = ledger
	s.cachedTransient = s.buildTransientSet(ledger)

	return s.cachedTransient
}

// advanceBefore advances past entries whose log range is entirely before
// logSeq. The resulting resume sequence is safe for a future
// newAppliedProposalSync call: it never skips an entry that could still
// cover logSeq.
func (s *appliedProposalSync) advanceBefore(logSeq uint64) {
	for s.current != nil && logSeq > s.maxLogSeq {
		s.resumeAfterSeq = s.currentSeq
		s.advance()
	}
}

// buildTransientSet returns the set of transient (account, asset) volumes
// declared by the current entry for the given ledger.
func (s *appliedProposalSync) buildTransientSet(ledger string) map[domain.AccountAssetKey]struct{} {
	if s.current == nil {
		return nil
	}

	volumeList, ok := s.current.GetTransientVolumes()[ledger]
	if !ok {
		return nil
	}

	return domain.TouchedVolumeSet(volumeList.GetVolumes())
}

func (s *appliedProposalSync) resumeSequence() uint64 {
	return s.resumeAfterSeq
}

// err returns the first non-EOF cursor error advance() observed, or nil
// if the sync only ever hit clean EOFs. Callers MUST check this between
// batches: a stashed error means the transient set the sync served may be
// incomplete, which would let the indexer persist mappings for volumes
// that should have been skipped.
func (s *appliedProposalSync) err() error {
	return s.iterErr
}

// close releases the underlying Pebble iterator.
func (s *appliedProposalSync) close() error {
	if s.cursor != nil {
		return s.cursor.Close()
	}

	return nil
}

// extractPurgedVolumes returns the set of purged ephemeral (account, asset)
// volumes declared by THIS log. Empty when the order that produced the log
// did not contribute to any ephemeral purge.
func extractPurgedVolumes(ledgerLog ledgerLogWithPurgedVolumes) map[domain.AccountAssetKey]struct{} {
	return domain.TouchedVolumeSet(ledgerLog.GetPurgedVolumes())
}

// ledgerLogWithPurgedVolumes narrows what we need from commonpb.LedgerLog so
// tests can pass either a real proto or a fake.
type ledgerLogWithPurgedVolumes interface {
	GetPurgedVolumes() []*commonpb.TouchedVolume
}

// excludedForLog returns the union of the transient (proposal-level) and
// purged (per-log) exclusion sets for the given log. This is the single
// helper used by process_logs.go (live path), indexLogEntry (backfill),
// and processBackfillPostings — the three previously inlined the same
// transientForLedger / extractPurgedVolumes / mergeExcluded combo. The
// sync is allowed to be nil so backfill callers without an
// AppliedProposal stream still get the purged side of the set.
func (s *appliedProposalSync) excludedForLog(logSeq uint64, ledger string, log ledgerLogWithPurgedVolumes) map[domain.AccountAssetKey]struct{} {
	if s == nil {
		return extractPurgedVolumes(log)
	}

	return mergeExcluded(
		s.transientForLedger(logSeq, ledger),
		extractPurgedVolumes(log),
	)
}

// mergeExcluded unions two (account, asset) exclusion sets without mutating
// either. The result is nil only when both inputs are empty.
func mergeExcluded(a, b map[domain.AccountAssetKey]struct{}) map[domain.AccountAssetKey]struct{} {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	out := make(map[domain.AccountAssetKey]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}

	return out
}
