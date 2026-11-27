package check

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type ledgerKVVerifier struct {
	values        map[string][]byte
	liveTruncated bool
}

func newLedgerKVVerifier() *ledgerKVVerifier {
	return &ledgerKVVerifier{values: make(map[string][]byte)}
}

func (v *ledgerKVVerifier) applyOrder(order *raftcmdpb.Order) {
	ls := order.GetLedgerScoped()
	if ls == nil {
		return
	}
	switch p := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_PutKv:
		v.values[string(dal.LedgerKVKey(ls.GetLedger(), p.PutKv.GetKey()))] = append([]byte(nil), p.PutKv.GetValue()...)
	case *raftcmdpb.LedgerScopedOrder_DeleteKv:
		delete(v.values, string(dal.LedgerKVKey(ls.GetLedger(), p.DeleteKv.GetKey())))
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		prefix := string(dal.NewKeyBuilder().PutZonePrefix(dal.ZonePerLedger, dal.SubPLKV).PutLedgerNameFixed(ls.GetLedger()).Build())
		for key := range v.values {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				delete(v.values, key)
			}
		}
	}
}

func (v *ledgerKVVerifier) compare(reader dal.PebbleReader, callback func(*servicepb.CheckStoreEvent)) error {
	if v.liveTruncated {
		return nil
	}
	prefix := []byte{dal.ZonePerLedger, dal.SubPLKV}
	iter, err := dal.NewBoundedIter(reader, prefix, dal.ZonePrefixUpperBound(dal.ZonePerLedger, dal.SubPLKV))
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	remaining := maps.Clone(v.values)
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if len(key) < len(prefix)+dal.LedgerNameFixedSize+1 {
			return errors.New("malformed ledger KV key")
		}
		value, ok := remaining[string(key)]
		ledger := string(bytes.TrimRight(key[2:2+dal.LedgerNameFixedSize], "\x00"))
		if !ok || !bytes.Equal(value, iter.Value()) {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_KV_MISMATCH,
				fmt.Sprintf("ledger KV %q on ledger %q differs from audited orders", key[2+dal.LedgerNameFixedSize:], ledger), 0, ledger, "", ""))
		}
		delete(remaining, string(key))
	}
	if err := iter.Error(); err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(remaining)) {
		keyBytes := []byte(key)
		ledger := string(bytes.TrimRight(keyBytes[2:2+dal.LedgerNameFixedSize], "\x00"))
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_KV_MISMATCH,
			fmt.Sprintf("audited ledger KV %q on ledger %q is missing", keyBytes[2+dal.LedgerNameFixedSize:], ledger), 0, ledger, "", ""))
	}

	return nil
}
