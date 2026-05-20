package readstore

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ledgerScopedPrefixes lists all readstore key prefixes that contain
// ledger-scoped data (keyed by [prefix...][ledgerID_BE_4B]...).
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
// [prefix...][ledgerID_BE_4B] -> [prefix...][(ledgerID+1)_BE_4B].
func DeleteLedgerIndexes(batch *dal.Batch, ledgerID uint32) error {
	var ledgerPrefix [4]byte
	binary.BigEndian.PutUint32(ledgerPrefix[:], ledgerID)

	var ledgerPrefixUpper [4]byte
	binary.BigEndian.PutUint32(ledgerPrefixUpper[:], ledgerID+1)

	for _, prefix := range ledgerScopedPrefixes {
		start := make([]byte, 0, len(prefix)+4)
		start = append(start, prefix...)
		start = append(start, ledgerPrefix[:]...)

		end := make([]byte, 0, len(prefix)+4)
		end = append(end, prefix...)
		end = append(end, ledgerPrefixUpper[:]...)

		if err := batch.DeleteRangeNoSync(start, end); err != nil {
			return fmt.Errorf("deleting readstore prefix %x for ledger %d: %w", prefix, ledgerID, err)
		}
	}

	return nil
}
