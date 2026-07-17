package usagestore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func newTestStore(t *testing.T) *usagestore.Store {
	t.Helper()

	s, err := usagestore.New(t.TempDir(), logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	return s
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
