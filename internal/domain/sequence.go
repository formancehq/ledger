package domain

import "math"

// SequenceCounter identifies the authoritative allocator that exhausted.
// These values are exposed as ErrorInfo metadata, so keep them stable.
type SequenceCounter string

const (
	SequenceCounterTransactionID SequenceCounter = "transactionId"
	SequenceCounterLedgerID      SequenceCounter = "ledgerId"
	SequenceCounterLedgerLogID   SequenceCounter = "ledgerLogId"
	SequenceCounterLog           SequenceCounter = "logSequence"
	SequenceCounterAudit         SequenceCounter = "auditSequence"
	SequenceCounterMirrorV2LogID SequenceCounter = "mirrorV2LogId"
)

// CheckedNextSequence returns the next uint64 sequence value without allowing
// modular wrap. MaxUint64 is deliberately not allocatable by counters that
// store the next value: accepting it would make the persisted next value zero
// and permit identifier/key reuse. See EN-1860.
func CheckedNextSequence(current uint64, counter SequenceCounter) (uint64, *ErrSequenceExhausted) {
	if current == math.MaxUint64 {
		return 0, &ErrSequenceExhausted{Counter: counter}
	}

	return current + 1, nil
}

// CheckedNextLedgerID checks that current is a safe ledger ID and returns
// current+1, or ErrSequenceExhausted when current==MaxUint32.
//
// The argument semantics differ by call site:
//   - Live path (processCreateLedger): current is the persisted next-ID
//     counter (the value that will be assigned to the new ledger). The return
//     is discarded; only the exhaustion check matters before IncrementNextLedgerID.
//   - Restore path (rebuildDelta): current is a CreatedLedgerLog.id that has
//     already been assigned. The return (current+1) is used as the minimum
//     value for the rebuilt next-ID counter.
//
// In both cases MaxUint32 is rejected because persisting it as the next value
// would wrap the uint32 counter to zero on the following allocation, permitting
// identifier reuse.
func CheckedNextLedgerID(current uint32) (uint32, *ErrSequenceExhausted) {
	if current == math.MaxUint32 {
		return 0, &ErrSequenceExhausted{Counter: SequenceCounterLedgerID}
	}

	return current + 1, nil
}
