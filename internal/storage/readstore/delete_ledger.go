package readstore

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ledgerScopedPrefixes lists all readstore prefix bytes that contain
// ledger-scoped data (keyed by [prefix][ledger\x00]...).
var ledgerScopedPrefixes = []byte{
	PrefixMetadataIndex,
	PrefixEntityExists,
	PrefixReverseMap,
	PrefixAccountTx,
	PrefixSourceAccountTx,
	PrefixDestAccountTx,
	PrefixTransactionReference,
	PrefixTransactionTimestamp,
	PrefixLedgerLogs,
	PrefixLedgerLogDate,
	PrefixTransactionInsertedAt,
	PrefixBackfill,
}

// DeleteLedgerIndexes removes all read index data for the given ledger.
// It performs range deletes on all ledger-scoped prefixes:
// [prefix][ledger\x00] -> [prefix][ledger\x01].
func DeleteLedgerIndexes(batch *dal.Batch, ledgerName string) error {
	ledgerPrefix := append([]byte(ledgerName), 0x00)
	ledgerPrefixUpper := IncrementBytes(ledgerPrefix)

	for _, prefix := range ledgerScopedPrefixes {
		start := append([]byte{prefix}, ledgerPrefix...)
		end := append([]byte{prefix}, ledgerPrefixUpper...)

		if err := batch.DeleteRangeNoSync(start, end); err != nil {
			return fmt.Errorf("deleting readstore prefix 0x%02x for ledger %q: %w", prefix, ledgerName, err)
		}
	}

	return nil
}
