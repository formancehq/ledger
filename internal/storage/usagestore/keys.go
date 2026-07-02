package usagestore

import (
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Pebble key prefixes for the usagebuilder's dedicated secondary store.
// All ledger-scoped keys follow [prefix][ledgerName padded 64B][...] so
// the comparer can build bloom filters on the ledger-scoped prefix — same
// pattern as the readstore.
const (
	// PrefixTemplate — per-template usage record.
	// Key: [0x01][ledgerName padded 64B][templateName] → TemplateUsage proto.
	PrefixTemplate byte = 0x01

	// PrefixCounter — per-ledger event counter.
	// Key: [0x02][ledgerName padded 64B][counterID] → uint64 BE.
	PrefixCounter byte = 0x02

	// PrefixInternal groups all non-ledger-scoped keys under a single prefix
	// so Comparer.Split treats them uniformly (full key = prefix).
	PrefixInternal byte = 0xFE

	// SubInternalProgress — [0xFE][0x01] → last consumed log sequence (uint64 BE).
	SubInternalProgress byte = 0x01
)

// Counter IDs identify each per-ledger event counter mirrored by the
// usagebuilder. Values are stable on-disk identifiers — never renumber.
const (
	CounterPosting            byte = 0x01 // posting_count            — sum len(Transaction.Postings) per CreatedTransaction / RevertedTransaction log
	CounterRevert             byte = 0x02 // revert_count             — count RevertTransactionOrder + MirrorRevertTransactionOrder
	CounterNumscriptExecution byte = 0x03 // numscript_execution_count — count CreateTransactionOrder with Script or NumscriptReference
	CounterReference          byte = 0x04 // reference_count          — count CreateTransactionOrder with non-empty Reference
	CounterEphemeralEvicted   byte = 0x05 // ephemeral_evicted_count  — sum len(LedgerLog.EphemeralVolumes) per log (pure ephemeral: new + purged same log)
	CounterTransientUsed      byte = 0x06 // transient_used_count     — sum len(AppliedProposal.TransientVolumes[ledger].Volumes) per proposal
	CounterVolume             byte = 0x07 // volume_count             — sum len(LedgerLog.NewKeptVolumes) - len(LedgerLog.PurgedVolumes) per log (ephemeral contributes 0)
)

// ProgressKey returns the full key for the usagebuilder progress entry.
//
//	[0xFE][0x01]
func ProgressKey() []byte {
	return []byte{PrefixInternal, SubInternalProgress}
}

// TemplateUsageKey builds the per-ledger, per-template usage entry key.
//
//	[0x01][ledgerName padded 64B][templateName]
func TemplateUsageKey(kb *dal.KeyBuilder, ledgerName, templateName string) []byte {
	return kb.Reset().
		PutByte(PrefixTemplate).
		PutLedgerNameFixed(ledgerName).
		PutString(templateName).
		Consume()
}

// CounterKey builds the per-ledger event counter key.
//
//	[0x02][ledgerName padded 64B][counterID]
func CounterKey(kb *dal.KeyBuilder, ledgerName string, counterID byte) []byte {
	return kb.Reset().
		PutByte(PrefixCounter).
		PutLedgerNameFixed(ledgerName).
		PutByte(counterID).
		Consume()
}
