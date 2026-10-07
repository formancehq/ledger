package check

import (
	"fmt"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ledgerMetadataVerifier folds successful chain-bound orders, never the mutable
// log or primary projection, into the expected ledger metadata keyspace.
type ledgerMetadataVerifier struct {
	values        map[string]map[string]*commonpb.MetadataValue
	liveTruncated bool
}

func newLedgerMetadataVerifier() *ledgerMetadataVerifier {
	return &ledgerMetadataVerifier{values: make(map[string]map[string]*commonpb.MetadataValue)}
}

func (v *ledgerMetadataVerifier) applyOrder(order *raftcmdpb.Order) {
	ls := order.GetLedgerScoped()
	ledger := ls.GetLedger()
	var metadata map[string]*commonpb.MetadataValue
	switch p := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		metadata = p.CreateLedger.GetMetadata()
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		metadata = p.SaveLedgerMetadata.GetMetadata()
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		delete(v.values[ledger], p.DeleteLedgerMetadata.GetKey())
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		delete(v.values, ledger)
	}
	for key, value := range metadata {
		if v.values[ledger] == nil {
			v.values[ledger] = make(map[string]*commonpb.MetadataValue)
		}
		v.values[ledger][key] = value.CloneVT()
	}
}

func (v *ledgerMetadataVerifier) compare(reader dal.PebbleReader, attrs *attributes.Attributes, callback func(*servicepb.CheckStoreEvent)) error {
	// A broken chain has already been reported; its prefix cannot substantiate
	// a comparison against the final projection.
	if v.liveTruncated {
		return nil
	}
	rows, err := attrs.LedgerMetadata.ComputeAllForPrefix(reader, nil)
	if err != nil {
		return err
	}
	remaining := make(map[domain.LedgerMetadataKey]*commonpb.MetadataValue)
	for ledger, metadata := range v.values {
		for key, value := range metadata {
			remaining[domain.LedgerMetadataKey{LedgerName: ledger, Key: key}] = value
		}
	}
	for _, row := range rows {
		var key domain.LedgerMetadataKey
		if err := key.Unmarshal(row.CanonicalKey); err != nil {
			return fmt.Errorf("decoding ledger metadata key: %w", err)
		}
		expected, exists := remaining[key]
		if !exists || !expected.EqualVT(row.Value) {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("ledger metadata %q on ledger %q differs from successful audited orders", key.Key, key.LedgerName), 0, key.LedgerName, "", key.Key))
		}
		delete(remaining, key)
	}
	keys := slices.Collect(maps.Keys(remaining))
	slices.SortFunc(keys, func(a, b domain.LedgerMetadataKey) int { return slices.Compare(a.Bytes(), b.Bytes()) })
	for _, key := range keys {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("audited ledger metadata %q on ledger %q is missing", key.Key, key.LedgerName), 0, key.LedgerName, "", key.Key))
	}

	return nil
}
