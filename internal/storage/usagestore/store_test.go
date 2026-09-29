package usagestore_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func newTestStore(t testing.TB) *usagestore.Store {
	t.Helper()

	s, err := usagestore.New(t.TempDir(), logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// BenchmarkStoreFlushCadence compares elapsed time for flushing every
// simulated second with the production 30-second cadence. Each
// operation models five minutes at 100 active ledgers, updating all seven
// counters and the progress cursor once per second.
func BenchmarkStoreFlushCadence(b *testing.B) {
	const (
		simulatedSeconds = 5 * 60
		ledgerCount      = 100
		counterCount     = 7
	)

	for _, flushEvery := range []int{1, 30} {
		b.Run(fmt.Sprintf("flush_every_%02ds", flushEvery), func(b *testing.B) {
			s := newTestStore(b)

			b.ResetTimer()
			for iteration := range b.N {
				for second := 1; second <= simulatedSeconds; second++ {
					batch := s.NewBatch()
					for ledgerIndex := range ledgerCount {
						ledger := fmt.Sprintf("ledger-%03d", ledgerIndex)
						for counterID := 1; counterID <= counterCount; counterID++ {
							require.NoError(b, s.PutCounter(batch, ledger, byte(counterID), uint64(iteration*simulatedSeconds+second)))
						}
					}
					require.NoError(b, s.WriteProgress(batch, uint64(iteration*simulatedSeconds+second)))
					require.NoError(b, batch.Commit())
					if second%flushEvery == 0 {
						require.NoError(b, s.Flush())
					}
				}
			}
			b.StopTimer()
		})
	}
}

func TestStore_ProgressRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	seq, err := s.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "fresh store must report cursor 0")

	batch := s.NewBatch()
	require.NoError(t, s.WriteProgress(batch, 42))
	require.NoError(t, batch.Commit())

	seq, err = s.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), seq)
}

func TestStore_CounterRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	v, err := s.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v, "missing counter must return 0, not error")

	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterPosting, 123))
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterRevert, 4))
	require.NoError(t, s.PutCounter(batch, "l2", usagestore.CounterPosting, 999))
	require.NoError(t, batch.Commit())

	posting, err := s.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(123), posting)

	revert, err := s.GetCounter("l1", usagestore.CounterRevert)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), revert)

	other, err := s.GetCounter("l2", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(999), other, "counters must be per-ledger scoped")
}

func TestStore_TemplateUsageRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	usage, err := s.GetTemplateUsage("l1", "missing")
	require.NoError(t, err)
	assert.Nil(t, usage, "missing template must return (nil, nil)")

	want := &commonpb.TemplateUsage{
		Count:    7,
		LastUsed: &commonpb.Timestamp{Data: 1_700_000_000_000_000_000},
	}

	batch := s.NewBatch()
	require.NoError(t, s.PutTemplateUsage(batch, "l1", "payout", want))
	require.NoError(t, batch.Commit())

	got, err := s.GetTemplateUsage("l1", "payout")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.GetCount(), got.GetCount())
	assert.Equal(t, want.GetLastUsed().GetData(), got.GetLastUsed().GetData())
}

func TestStore_ClosePersistsProgressCountersAndTemplates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)

	wantUsage := &commonpb.TemplateUsage{
		Count:    7,
		LastUsed: &commonpb.Timestamp{Data: 1_700_000_000_000_000_000},
	}
	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterPosting, 123))
	require.NoError(t, s.PutTemplateUsage(batch, "l1", "payout", wantUsage))
	require.NoError(t, s.WriteProgress(batch, 42))
	require.NoError(t, batch.Commit())
	require.NoError(t, s.Close())

	reopened, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	progress, err := reopened.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), progress)

	counter, err := reopened.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(123), counter)

	usage, err := reopened.GetTemplateUsage("l1", "payout")
	require.NoError(t, err)
	require.NotNil(t, usage)
	assert.Equal(t, wantUsage.GetCount(), usage.GetCount())
	assert.Equal(t, wantUsage.GetLastUsed().GetData(), usage.GetLastUsed().GetData())
}

func TestStore_CloseReadOnlyDoesNotFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writable, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)

	batch := writable.NewBatch()
	require.NoError(t, writable.WriteProgress(batch, 42))
	require.NoError(t, batch.Commit())
	require.NoError(t, writable.Close())

	readOnly, err := usagestore.OpenReadOnly(filepath.Join(dir, "usagedb"), logging.NopZap())
	require.NoError(t, err)

	progress, err := readOnly.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(42), progress)
	require.NoError(t, readOnly.Close(), "closing a read-only store must not attempt a flush")
}

func TestStore_DeleteLedgerCascade(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Seed both scopes for two ledgers.
	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterPosting, 10))
	require.NoError(t, s.PutTemplateUsage(batch, "l1", "t1", &commonpb.TemplateUsage{Count: 3}))
	require.NoError(t, s.PutCounter(batch, "l2", usagestore.CounterPosting, 20))
	require.NoError(t, s.PutTemplateUsage(batch, "l2", "t2", &commonpb.TemplateUsage{Count: 5}))
	require.NoError(t, batch.Commit())

	// Drop l1 only.
	batch = s.NewBatch()
	require.NoError(t, usagestore.DeleteLedger(batch, "l1"))
	require.NoError(t, batch.Commit())

	// l1 is gone.
	v, err := s.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v)

	tu, err := s.GetTemplateUsage("l1", "t1")
	require.NoError(t, err)
	assert.Nil(t, tu)

	// l2 survives.
	v, err = s.GetCounter("l2", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), v)

	tu, err = s.GetTemplateUsage("l2", "t2")
	require.NoError(t, err)
	require.NotNil(t, tu)
	assert.Equal(t, uint64(5), tu.GetCount())
}

// TestStore_Reset guards the primary-store-rollback recovery path: Reset must
// wipe every counter + template row across all ledgers AND clear the progress
// cursor so the builder replays from audit sequence 0.
func TestStore_Reset(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Seed counters + templates for two ledgers, plus a progress cursor
	// simulating a projection that had consumed 500 audit entries.
	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterPosting, 10))
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterVolume, 3))
	require.NoError(t, s.PutTemplateUsage(batch, "l1", "t1", &commonpb.TemplateUsage{Count: 3}))
	require.NoError(t, s.PutCounter(batch, "l2", usagestore.CounterPosting, 20))
	require.NoError(t, s.PutTemplateUsage(batch, "l2", "t2", &commonpb.TemplateUsage{Count: 5}))
	require.NoError(t, s.WriteProgress(batch, 500))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.Reset())

	// Every counter across both ledgers reads 0.
	for _, ledger := range []string{"l1", "l2"} {
		for _, counter := range []byte{usagestore.CounterPosting, usagestore.CounterVolume} {
			v, err := s.GetCounter(ledger, counter)
			require.NoError(t, err)
			assert.Equal(t, uint64(0), v, "counter %#x for %q must be wiped by Reset", counter, ledger)
		}
	}

	// Every template row is gone.
	for _, tk := range []struct{ ledger, tpl string }{{"l1", "t1"}, {"l2", "t2"}} {
		tu, err := s.GetTemplateUsage(tk.ledger, tk.tpl)
		require.NoError(t, err)
		assert.Nil(t, tu, "template %q/%q must be wiped by Reset", tk.ledger, tk.tpl)
	}

	// The cursor is back to 0 so the next boot replays from the start.
	seq, err := s.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "Reset must clear the progress cursor")

	// Reset on an already-empty store is a no-op, not an error.
	require.NoError(t, s.Reset())
}

func TestStore_ResetFlushesBeforeReturn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)

	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "l1", usagestore.CounterPosting, 10))
	require.NoError(t, s.PutTemplateUsage(batch, "l1", "t1", &commonpb.TemplateUsage{Count: 3}))
	require.NoError(t, s.WriteProgress(batch, 500))
	require.NoError(t, batch.Commit())
	require.NoError(t, s.Flush(), "seed the old cursor in an SST before resetting")

	require.NoError(t, s.Reset())

	require.NoError(t, s.Close())

	reopened, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	progress, err := reopened.ReadProgress()
	require.NoError(t, err)
	assert.Zero(t, progress)

	counter, err := reopened.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Zero(t, counter)

	usage, err := reopened.GetTemplateUsage("l1", "t1")
	require.NoError(t, err)
	assert.Nil(t, usage)
}

func TestStore_CheckpointIncludesUnflushedRows(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, s.WriteProgress(batch, 77))
	require.NoError(t, s.PutCounter(batch, "ledger", usagestore.CounterPosting, 9))
	require.NoError(t, batch.Commit())
	dest := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, s.CreateCheckpoint(dest))
	require.Error(t, s.CreateCheckpoint(dest), "existing destination must not be overwritten")
	checkpoint, err := usagestore.OpenReadOnly(dest, logging.NopZap())
	require.NoError(t, err)
	defer func() { require.NoError(t, checkpoint.Close()) }()
	progress, err := checkpoint.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(77), progress)
	counter, err := checkpoint.GetCounter("ledger", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), counter)
}

func TestStore_SnapshotPinsCounterAndTemplate(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "ledger", usagestore.CounterPosting, 1))
	require.NoError(t, s.PutTemplateUsage(batch, "ledger", "template", &commonpb.TemplateUsage{Count: 1}))
	require.NoError(t, batch.Commit())
	snapshot := s.NewSnapshot()
	defer func() { require.NoError(t, snapshot.Close()) }()
	batch = s.NewBatch()
	require.NoError(t, s.PutCounter(batch, "ledger", usagestore.CounterPosting, 2))
	require.NoError(t, s.PutTemplateUsage(batch, "ledger", "template", &commonpb.TemplateUsage{Count: 2}))
	require.NoError(t, batch.Commit())
	counter, err := snapshot.GetCounter("ledger", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), counter)
	usage, err := snapshot.GetTemplateUsage("ledger", "template")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), usage.GetCount())
}

func TestStore_BatchTerminalState(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("x"), []byte("y")))
	require.NoError(t, batch.Commit())
	require.Error(t, batch.SetBytes([]byte("x"), []byte("z")))
	require.Error(t, batch.Commit())
	require.NoError(t, batch.Cancel())
	cancelled := s.NewBatch()
	require.NoError(t, cancelled.Cancel())
	require.Error(t, cancelled.SetBytes([]byte("x"), []byte("z")))
}

// TestStore_LedgerPrefixBloom verifies the persisted RocksDB prefix and filter
// configuration, then exercises a prefix-restricted iterator across two ledgers.
func TestStore_LedgerPrefixBloom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := usagestore.New(dir, logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	batch := s.NewBatch()
	for _, row := range []struct {
		ledger  string
		counter byte
	}{
		{"alpha", usagestore.CounterPosting},
		{"alpha", usagestore.CounterRevert},
		{"beta", usagestore.CounterPosting},
	} {
		require.NoError(t, s.PutCounter(batch, row.ledger, row.counter, 1))
	}
	require.NoError(t, batch.Commit())
	require.NoError(t, s.Flush())

	files, err := filepath.Glob(filepath.Join(dir, "usagedb", "OPTIONS-*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	options, err := os.ReadFile(files[len(files)-1])
	require.NoError(t, err)
	assert.Contains(t, string(options), "prefix_extractor=rocksdb.CappedPrefix.65")
	assert.Contains(t, string(options), "filter_policy=bloomfilter")
	assert.Contains(t, string(options), "whole_key_filtering=false")

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetPrefixSameAsStart(true)
	iter := s.DB().NewIterator(ro)
	key := usagestore.CounterKey(dal.NewKeyBuilder(), "alpha", usagestore.CounterPosting)
	iter.Seek(key)
	var got []byte
	for iter.Valid() {
		got = append(got, iter.Key().Data()[len(key)-1])
		iter.Next()
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, []byte{usagestore.CounterPosting, usagestore.CounterRevert}, got,
		"prefix iterator must include both alpha counters and stop before beta")
	iter.Close()
	ro.Destroy()
	require.NoError(t, s.Close())

	reopened, err := usagestore.OpenReadOnly(filepath.Join(dir, "usagedb"), logging.NopZap())
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	_, err = reopened.GetCounter("alpha", usagestore.CounterPosting)
	require.NoError(t, err)
}

func TestStore_CompressionPerLevel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := usagestore.DefaultConfig()
	cfg.Compression[0] = pebblecfg.NoCompression
	cfg.Compression[1] = pebblecfg.ZstdCompression
	s, err := usagestore.New(dir, logging.NopZap(), cfg)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	files, err := filepath.Glob(filepath.Join(dir, "usagedb", "OPTIONS-*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	options, err := os.ReadFile(files[len(files)-1])
	require.NoError(t, err)
	var compression string
	for _, line := range strings.Split(string(options), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "compression_per_level="); ok {
			compression = value
			break
		}
	}
	require.NotEmpty(t, compression)
	codecs := strings.Split(compression, ":")
	require.Len(t, codecs, pebblecfg.NumLevels)
	assert.Equal(t, "kNoCompression", codecs[0])
	assert.Equal(t, "kZSTD", codecs[1])
}
