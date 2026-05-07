package indexbuilder

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// auditSync maintains a second Pebble iterator over the audit stream ([0x02])
// and provides transient account lookups for the current proposal.
//
// The sync algorithm is a merge of two sorted streams:
//   - The log iterator advances sequentially (handled by processLogs).
//   - The audit iterator runs ahead; for each AuditSuccess, it caches the
//     log_sequences range and transient_accounts map.
//
// When processLogs encounters a log whose sequence falls within the current
// audit's range, the transient set is available. When the log sequence exceeds
// the range, the audit iterator advances to the next AuditSuccess.
type auditSync struct {
	// Current loaded audit entry (nil if exhausted or not yet loaded).
	current *auditpb.AuditSuccess

	// Log sequence range of the current audit entry.
	minLogSeq uint64
	maxLogSeq uint64

	// Audit sequence of the current entry (for progress persistence).
	currentAuditSeq uint64

	// Cached excluded set to avoid rebuilding for every log in the same proposal.
	cachedLedger   string
	cachedExcluded map[string]struct{}

	// Pebble iterator and cursor state.
	cursor    dal.Cursor[*auditpb.AuditEntry]
	exhausted bool
}

// newAuditSync creates an auditSync that reads audit entries from the given
// Pebble handle, starting after afterAuditSeq.
func newAuditSync(handle dal.PebbleReader, afterAuditSeq uint64) (*auditSync, error) {
	var filter *uint64
	if afterAuditSeq > 0 {
		filter = &afterAuditSeq
	}

	cursor, err := query.ReadAuditEntries(context.Background(), handle, filter)
	if err != nil {
		return nil, err
	}

	as := &auditSync{cursor: cursor}
	// Pre-load the first AuditSuccess.
	as.advance()

	return as, nil
}

// advance moves to the next AuditSuccess entry, skipping failures.
func (as *auditSync) advance() {
	for {
		entry, err := as.cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				as.exhausted = true
				as.current = nil
			}

			return
		}

		success := entry.GetSuccess()
		if success == nil || success.GetMaxLogSequence() == 0 {
			// Skip failure entries (no log sequences).
			as.currentAuditSeq = entry.GetSequence()

			continue
		}

		as.current = success
		as.currentAuditSeq = entry.GetSequence()
		as.minLogSeq = success.GetMinLogSequence()
		as.maxLogSeq = success.GetMaxLogSequence()
		as.cachedLedger = ""
		as.cachedExcluded = nil

		return
	}
}

// syncTo advances the audit iterator until the current entry covers logSeq.
// Returns the set of accounts to exclude from indexing (transient + purged
// ephemeral) for the given ledger, or nil.
func (as *auditSync) syncTo(logSeq uint64, ledger string) map[string]struct{} {
	if as.exhausted {
		return nil
	}

	// Advance past entries whose log range is entirely before logSeq.
	for as.current != nil && logSeq > as.maxLogSeq {
		as.advance()
	}

	if as.current == nil {
		return nil
	}

	// Check if logSeq falls within the current audit's range.
	if logSeq < as.minLogSeq || logSeq > as.maxLogSeq {
		return nil
	}

	// Return cached set if same ledger within the same audit entry.
	if as.cachedLedger == ledger {
		return as.cachedExcluded
	}

	as.cachedLedger = ledger
	as.cachedExcluded = as.excludedAccountsForLedger(ledger)

	return as.cachedExcluded
}

// excludedAccountsForLedger returns a set of account addresses that should be
// excluded from indexing for the given ledger: transient accounts (never
// persisted) and purged ephemeral accounts (zero balance after commit).
func (as *auditSync) excludedAccountsForLedger(ledger string) map[string]struct{} {
	if as.current == nil {
		return nil
	}

	var set map[string]struct{}

	if ta := as.current.GetTransientAccounts(); ta != nil {
		if accountList, ok := ta[ledger]; ok && accountList != nil {
			for _, account := range accountList.GetAccounts() {
				if set == nil {
					set = make(map[string]struct{})
				}

				set[account] = struct{}{}
			}
		}
	}

	if pa := as.current.GetPurgedAccounts(); pa != nil {
		if accountList, ok := pa[ledger]; ok && accountList != nil {
			for _, account := range accountList.GetAccounts() {
				if set == nil {
					set = make(map[string]struct{})
				}

				set[account] = struct{}{}
			}
		}
	}

	return set
}

// auditSequence returns the last consumed audit sequence.
func (as *auditSync) auditSequence() uint64 {
	return as.currentAuditSeq
}

// close releases the underlying Pebble iterator.
func (as *auditSync) close() error {
	if as.cursor != nil {
		return as.cursor.Close()
	}

	return nil
}
