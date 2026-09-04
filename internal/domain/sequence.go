package domain

import "math"

// SequenceCounter identifies the authoritative allocator that exhausted.
// These values are exposed as ErrorInfo metadata, so keep them stable.
type SequenceCounter string

const (
	SequenceCounterTransactionID SequenceCounter = "transactionId"
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
