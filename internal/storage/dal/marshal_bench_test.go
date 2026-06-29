package dal

import (
	"fmt"
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Benchmarks compare the cost of WriteSession.MarshalProto in non-deterministic
// (default) vs deterministic mode, on representative persisted message shapes:
//
//   - VolumePair       : map-free, small, hottest type in the FSM apply path
//                        (one per touched account/asset per order).
//   - TransactionState : map-bearing (Metadata), medium-size; written for
//                        every produced transaction.
//   - LedgerInfo       : map-bearing (Metadata, AccountTypes); the heaviest
//                        of the attribute-zone types.
//   - Transaction      : composite with repeated Postings + Metadata map;
//                        stand-in for a typical apply payload.
//
// Each benchmark runs marshal-only (no Pebble I/O), so the gap measures
// the cost of MarshalToSizedBufferDeterministicVT (map-key sort) vs
// MarshalToSizedBufferVT in isolation. For map-free messages both code
// paths land on MarshalToSizedBufferVT — we expect zero difference.

func benchStore(b *testing.B, deterministic bool) *Store {
	b.Helper()

	cfg := DefaultConfig()
	cfg.DeterministicEncoding = deterministic

	store, err := NewStore(b.TempDir(), logging.Testing(), noop.Meter{}, cfg)
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}

	b.Cleanup(func() { _ = store.Close() })

	return store
}

// ---- Map-free shape: VolumePair ----

func newVolumePair() *raftcmdpb.VolumePair {
	return &raftcmdpb.VolumePair{
		Input:  &commonpb.Uint256{V0: 0xABCDEF, V1: 0x123456},
		Output: &commonpb.Uint256{V0: 0xFEDCBA, V1: 0x654321},
	}
}

func BenchmarkMarshalProto_VolumePair_NonDeterministic(b *testing.B) {
	store := benchStore(b, false)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()
	msg := newVolumePair()
	b.ResetTimer()

	for range b.N {
		if _, err := sess.MarshalProto(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalProto_VolumePair_Deterministic(b *testing.B) {
	store := benchStore(b, true)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()
	msg := newVolumePair()
	b.ResetTimer()

	for range b.N {
		if _, err := sess.MarshalProto(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- Medium map-bearing shape: TransactionState ----

func newTransactionState(metadataSize int) *commonpb.TransactionState {
	tx := &commonpb.TransactionState{
		CreatedByLog:          42,
		RevertedByTransaction: 0,
		Timestamp:             &commonpb.Timestamp{Data: 1_700_000_000_000_000},
		Metadata:              make(map[string]*commonpb.MetadataValue, metadataSize),
	}

	for i := range metadataSize {
		key := "field_" + strconv.Itoa(i)
		tx.Metadata[key] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{
				StringValue: "value-" + strconv.Itoa(i) + "-the-quick-brown-fox-jumps-over-the-lazy-dog",
			},
		}
	}

	return tx
}

func benchTransactionState(b *testing.B, deterministic bool, metadataSize int) {
	store := benchStore(b, deterministic)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()
	msg := newTransactionState(metadataSize)
	b.ResetTimer()

	for range b.N {
		if _, err := sess.MarshalProto(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalProto_TransactionState(b *testing.B) {
	sizes := []int{5, 50, 500}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("metadata=%d/non-det", sz), func(b *testing.B) {
			benchTransactionState(b, false, sz)
		})
		b.Run(fmt.Sprintf("metadata=%d/det", sz), func(b *testing.B) {
			benchTransactionState(b, true, sz)
		})
	}
}

// ---- Heavy map-bearing shape: LedgerInfo ----

func newLedgerInfo(metaSize, accountTypeSize int) *commonpb.LedgerInfo {
	li := &commonpb.LedgerInfo{
		Name:                   "main-ledger",
		Id:                     7,
		Mode:                   commonpb.LedgerMode_LEDGER_MODE_NORMAL,
		CreatedAt:              &commonpb.Timestamp{Data: 1_700_000_000_000_000},
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		Metadata:               make(map[string]*commonpb.MetadataValue, metaSize),
		AccountTypes:           make(map[string]*commonpb.AccountType, accountTypeSize),
	}

	for i := range metaSize {
		key := "meta_" + strconv.Itoa(i)
		li.Metadata[key] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: "v-" + key},
		}
	}

	for i := range accountTypeSize {
		key := "acct_type_" + strconv.Itoa(i)
		li.AccountTypes[key] = &commonpb.AccountType{
			Name:    key,
			Pattern: key + ":*",
		}
	}

	return li
}

func benchLedgerInfo(b *testing.B, deterministic bool, metaSize, atSize int) {
	store := benchStore(b, deterministic)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()
	msg := newLedgerInfo(metaSize, atSize)
	b.ResetTimer()

	for range b.N {
		if _, err := sess.MarshalProto(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalProto_LedgerInfo(b *testing.B) {
	cases := []struct {
		meta, at int
	}{
		{5, 3},
		{50, 20},
		{500, 100},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("meta=%d_acct=%d/non-det", c.meta, c.at), func(b *testing.B) {
			benchLedgerInfo(b, false, c.meta, c.at)
		})
		b.Run(fmt.Sprintf("meta=%d_acct=%d/det", c.meta, c.at), func(b *testing.B) {
			benchLedgerInfo(b, true, c.meta, c.at)
		})
	}
}

// ---- Composite: Transaction with repeated Postings + Metadata map ----

func newTransaction(postings, metaSize int) *commonpb.Transaction {
	postingsList := make([]*commonpb.Posting, postings)
	for i := range postings {
		postingsList[i] = &commonpb.Posting{
			Source:      "world",
			Destination: "users:" + strconv.Itoa(i),
			Amount:      &commonpb.Uint256{V0: uint64(100 * (i + 1))},
			Asset:       "USD/2",
		}
	}

	meta := make(map[string]*commonpb.MetadataValue, metaSize)
	for i := range metaSize {
		key := "k" + strconv.Itoa(i)
		meta[key] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: "v-" + key + "-padding-padding"},
		}
	}

	return &commonpb.Transaction{
		Id:        9876,
		Reference: "tx-bench",
		Timestamp: &commonpb.Timestamp{Data: 1_700_000_000_000_000},
		Postings:  postingsList,
		Metadata:  meta,
	}
}

func benchTransaction(b *testing.B, deterministic bool, postings, metaSize int) {
	store := benchStore(b, deterministic)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()
	msg := newTransaction(postings, metaSize)
	b.ResetTimer()

	for range b.N {
		if _, err := sess.MarshalProto(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalProto_Transaction(b *testing.B) {
	cases := []struct {
		postings, meta int
	}{
		{2, 5},
		{10, 20},
		{100, 100},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("postings=%d_meta=%d/non-det", c.postings, c.meta), func(b *testing.B) {
			benchTransaction(b, false, c.postings, c.meta)
		})
		b.Run(fmt.Sprintf("postings=%d_meta=%d/det", c.postings, c.meta), func(b *testing.B) {
			benchTransaction(b, true, c.postings, c.meta)
		})
	}
}
