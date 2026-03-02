package query

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadPreparedQuery reads a single prepared query by ledger and name.
func ReadPreparedQuery(reader dal.PebbleReader, ledger, name string) (*commonpb.PreparedQuery, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixPreparedQuery)
	kb.PutLedgerName(ledger)
	kb.PutString(name)
	key := kb.Build()

	val, closer, err := reader.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("reading prepared query %s/%s: %w", ledger, name, err)
	}
	defer func() { _ = closer.Close() }()

	pq := &commonpb.PreparedQuery{}
	if err := pq.UnmarshalVT(val); err != nil {
		return nil, fmt.Errorf("unmarshaling prepared query %s/%s: %w", ledger, name, err)
	}

	return pq, nil
}

// ReadPreparedQueries reads all prepared queries for a ledger.
func ReadPreparedQueries(reader dal.PebbleReader, ledger string) ([]*commonpb.PreparedQuery, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixPreparedQuery)
	kb.PutLedgerName(ledger)
	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixPreparedQuery)
	kb2.PutLedgerName(ledger)
	kb2.PutBytes([]byte{0xFF})
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for prepared queries: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var queries []*commonpb.PreparedQuery
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading prepared query value: %w", err)
		}

		pq := &commonpb.PreparedQuery{}
		if err := pq.UnmarshalVT(val); err != nil {
			return nil, fmt.Errorf("unmarshaling prepared query: %w", err)
		}
		queries = append(queries, pq)
	}

	return queries, nil
}

// WritePreparedQuery writes a prepared query using a dal.Batch.
func WritePreparedQuery(batch *dal.Batch, pq *commonpb.PreparedQuery) error {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixPreparedQuery)
	kb.PutLedgerName(pq.Ledger)
	kb.PutString(pq.Name)
	key := kb.Build()

	return batch.SetProto(key, pq)
}

// DeletePreparedQuery deletes a prepared query using a dal.Batch.
func DeletePreparedQuery(batch *dal.Batch, ledger, name string) error {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixPreparedQuery)
	kb.PutLedgerName(ledger)
	kb.PutString(name)
	key := kb.Build()

	return batch.DeleteKey(key)
}
