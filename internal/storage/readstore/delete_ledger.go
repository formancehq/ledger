package readstore

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ledgerScopedPrefixes lists all readstore key prefixes that contain
// ledger-scoped data (keyed by [prefix...][ledger\x00]...).
var ledgerScopedPrefixes = [][]byte{
	{PrefixMetadataIndex},
	{PrefixEntityExists},
	{PrefixReverseMap},
	{PrefixAccountTx},
	{PrefixSourceAccountTx},
	{PrefixDestAccountTx},
	{PrefixTransactionReference},
	{PrefixTransactionTimestamp},
	{PrefixLedgerLogs},
	{PrefixLedgerLogDate},
	{PrefixTransactionInsertedAt},
	{PrefixInternal, SubInternalBackfill},
}

// DeleteLedgerIndexes removes all read index data for the given ledger.
// It performs range deletes on all ledger-scoped prefixes:
// [prefix...][ledger\x00] -> [prefix...][ledger\x01].
func DeleteLedgerIndexes(batch *dal.Batch, ledgerName string) error {
	ledgerPrefix := append([]byte(ledgerName), 0x00)
	ledgerPrefixUpper := IncrementBytes(ledgerPrefix)

	for _, prefix := range ledgerScopedPrefixes {
		start := make([]byte, 0, len(prefix)+len(ledgerPrefix))
		start = append(start, prefix...)
		start = append(start, ledgerPrefix...)

		end := make([]byte, 0, len(prefix)+len(ledgerPrefixUpper))
		end = append(end, prefix...)
		end = append(end, ledgerPrefixUpper...)

		if err := batch.DeleteRangeNoSync(start, end); err != nil {
			return fmt.Errorf("deleting readstore prefix %x for ledger %q: %w", prefix, ledgerName, err)
		}
	}

	return nil
}
