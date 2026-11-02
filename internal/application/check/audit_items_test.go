package check

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCheckAuditItemPhysicalIdentity(t *testing.T) {
	t.Parallel()
	for _, orphan := range []bool{true, false} {
		name := "physical index differs from value"
		if orphan {
			name = "zero-log orphan"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			engine := newTestEngine(t)
			engine.processAndCommit(createLedgerOrder("main"))
			batch := engine.store.OpenWriteSession()
			defer func() { _ = batch.Cancel() }()
			if orphan {
				require.NoError(t, batch.SetProto(auditItemTestKey(99, 0), &auditpb.AuditItem{}))
			} else {
				value, closer, err := engine.store.Get(auditItemTestKey(1, 0))
				require.NoError(t, err)
				require.NoError(t, batch.SetBytes(auditItemTestKey(1, 1), value))
				require.NoError(t, closer.Close())
				require.NoError(t, batch.DeleteKey(auditItemTestKey(1, 0)))
			}
			require.NoError(t, batch.Commit())
			findings := collectCheckErrors(t, engine.store, engine.attrs)
			structural := errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH)
			require.NotEmpty(t, structural, "full Check must reject invalid physical audit item identity")
		})
	}
}

func auditItemTestKey(sequence uint64, index uint32) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(sequence).PutUint32(index).Build()
}

func TestVerifyAuditItemsStructuralCases(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		key       []byte
		item      *auditpb.AuditItem
		parentKey uint64
		parent    *auditpb.AuditEntry
		want      string
	}{
		{name: "healthy zero-log item", key: auditItemTestKey(7, 0), item: &auditpb.AuditItem{}, parentKey: 7, parent: &auditpb.AuditEntry{Sequence: 7}},
		{name: "healthy maximum sequence", key: auditItemTestKey(math.MaxUint64, 0), item: &auditpb.AuditItem{}, parentKey: math.MaxUint64, parent: &auditpb.AuditEntry{Sequence: math.MaxUint64}},
		{name: "missing parent", key: auditItemTestKey(7, 0), item: &auditpb.AuditItem{}, want: "missing parent audit entry"},
		{name: "missing maximum parent", key: auditItemTestKey(math.MaxUint64, 0), item: &auditpb.AuditItem{}, want: "missing parent audit entry"},
		{name: "zero parent", key: auditItemTestKey(0, 0), item: &auditpb.AuditItem{}, want: "reserved parent sequence zero"},
		{name: "parent identity", key: auditItemTestKey(7, 0), item: &auditpb.AuditItem{}, parentKey: 7, parent: &auditpb.AuditEntry{Sequence: 8}, want: "parent key and payload sequence differ"},
		{name: "item index", key: auditItemTestKey(7, 3), item: &auditpb.AuditItem{OrderIndex: 2}, parentKey: 7, parent: &auditpb.AuditEntry{Sequence: 7}, want: "physical order index and item order index differ"},
		{name: "bare prefix", key: []byte{dal.ZoneHistory, dal.SubHistoryAuditItem}, item: &auditpb.AuditItem{}, want: "invalid physical key length"},
		{name: "short key", key: auditItemTestKey(7, 0)[:13], item: &auditpb.AuditItem{}, want: "invalid physical key length"},
		{name: "long key", key: append(auditItemTestKey(7, 0), 0), item: &auditpb.AuditItem{}, want: "invalid physical key length"},
		{name: "large key diagnostic", key: append(auditItemTestKey(7, 0), make([]byte, 10000)...), item: &auditpb.AuditItem{}, want: "invalid physical key length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			batch := store.OpenWriteSession()
			require.NoError(t, batch.SetProto(tc.key, tc.item))
			if tc.parent != nil {
				require.NoError(t, batch.SetProto(auditParentTestKey(tc.parentKey), tc.parent))
			}
			require.NoError(t, batch.Commit())
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			findings, err := runAuditItemsTest(context.Background(), reader)
			require.NoError(t, err)
			if tc.want == "" {
				require.Empty(t, findings)
			} else {
				require.Len(t, findings, 1)
				require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH, findings[0].GetErrorType())
				require.Contains(t, findings[0].GetMessage(), tc.want)
				require.Less(t, len(findings[0].GetMessage()), 1000)
			}
		})
	}
}

func TestVerifyAuditItemsParentLookupsAndBoundedDiagnostics(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	batch := store.OpenWriteSession()
	// A large declared OrderCount must not cause a walk of absent item positions.
	require.NoError(t, batch.SetProto(auditParentTestKey(7), &auditpb.AuditEntry{Sequence: 7, OrderCount: math.MaxUint32}))
	for _, seq := range []uint64{7, math.MaxUint64} {
		for i := range uint32(100) {
			require.NoError(t, batch.SetProto(auditItemTestKey(seq, i), &auditpb.AuditItem{OrderIndex: i}))
		}
	}
	require.NoError(t, batch.Commit())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	counted := &auditItemsTestReader{PebbleReader: reader}
	findings, err := runAuditItemsTest(context.Background(), counted)
	require.NoError(t, err)
	require.Equal(t, 2, counted.gets, "one lookup per encountered parent, including absent parents")
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].GetMessage(), "100 audit item rows")
	require.Equal(t, 4, strings.Count(findings[0].GetMessage(), "key "))
	require.Less(t, len(findings[0].GetMessage()), 1000)
}

func TestVerifyAuditItemsFailures(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(auditItemTestKey(7, 0), &auditpb.AuditItem{}))
	require.NoError(t, batch.Commit())
	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	sentinel := errors.New("injected storage failure")
	t.Run("iterator creation", func(t *testing.T) {
		_, err := runAuditItemsTest(context.Background(), &auditItemsTestReader{PebbleReader: reader, iterErr: sentinel})
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("parent lookup", func(t *testing.T) {
		_, err := runAuditItemsTest(context.Background(), &auditItemsTestReader{PebbleReader: reader, getErr: sentinel})
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("canceled before iteration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		counted := &auditItemsTestReader{PebbleReader: reader}
		_, err := runAuditItemsTest(ctx, counted)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, counted.gets)
	})
	t.Run("canceled during iteration", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := runAuditItemsTest(ctx, &auditItemsTestReader{PebbleReader: reader, afterGet: cancel})
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("invalid item value", func(t *testing.T) {
		batch := store.OpenWriteSession()
		require.NoError(t, batch.SetBytes(auditItemTestKey(7, 1), []byte{0xff}))
		require.NoError(t, batch.Commit())
		fresh, err := store.NewReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, fresh.Close()) }()
		_, err = runAuditItemsTest(context.Background(), fresh)
		require.ErrorContains(t, err, "decoding audit item membership value")
	})
}

// The wrapper counts real snapshot lookups and injects storage failures; it
// delegates normal reads and iteration to Pebble rather than modeling storage.
type auditItemsTestReader struct {
	dal.PebbleReader

	gets     int
	getErr   error
	iterErr  error
	afterGet func()
}

func (r *auditItemsTestReader) Get(key []byte) ([]byte, io.Closer, error) {
	r.gets++
	if r.getErr != nil {
		return nil, nil, r.getErr
	}
	if r.afterGet != nil {
		defer r.afterGet()
	}

	return r.PebbleReader.Get(key)
}

func (r *auditItemsTestReader) NewIter(options *pebble.IterOptions) (*pebble.Iterator, error) {
	if r.iterErr != nil {
		return nil, r.iterErr
	}

	return r.PebbleReader.NewIter(options)
}

func auditParentTestKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(sequence).Build()
}

func runAuditItemsTest(ctx context.Context, reader dal.PebbleReader) ([]*servicepb.CheckStoreError, error) {
	var findings []*servicepb.CheckStoreError
	checker := NewChecker(nil, attributes.New(), nil, logging.Testing())
	err := checker.verifyAuditItems(ctx, reader, func(event *servicepb.CheckStoreEvent) {
		if finding := event.GetError(); finding != nil {
			findings = append(findings, finding)
		}
	})

	return findings, err
}

func BenchmarkVerifyAuditItems(b *testing.B) {
	for _, parents := range []int{1, 1000} {
		name := "shared parent"
		if parents > 1 {
			name = "distinct parents"
		}
		b.Run(name, func(b *testing.B) {
			store, err := dal.NewStore(b.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("benchmark"), dal.DefaultConfig())
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			batch := store.OpenWriteSession()
			for parent := range parents {
				seq := uint64(parent + 1)
				require.NoError(b, batch.SetProto(auditParentTestKey(seq), &auditpb.AuditEntry{Sequence: seq}))
				for index := range uint32(1000 / parents) {
					require.NoError(b, batch.SetProto(auditItemTestKey(seq, index), &auditpb.AuditItem{OrderIndex: index}))
				}
			}
			require.NoError(b, batch.Commit())
			reader, err := store.NewReadHandle()
			require.NoError(b, err)
			defer func() { require.NoError(b, reader.Close()) }()
			checker := NewChecker(nil, attributes.New(), nil, logging.Testing())
			counted := &auditItemsTestReader{PebbleReader: reader}
			callback := func(*servicepb.CheckStoreEvent) { b.Fatal("healthy scan emitted finding") }
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				require.NoError(b, checker.verifyAuditItems(context.Background(), counted, callback))
			}
			b.StopTimer()
			b.ReportMetric(float64(counted.gets)/float64(b.N), "parent-reads/op")
		})
	}
}
