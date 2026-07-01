package usagestore_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func newTestStore(t *testing.T) *usagestore.Store {
	t.Helper()

	s, err := usagestore.New(t.TempDir(), discardLogger{}, usagestore.DefaultConfig())
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

// discardLogger mirrors readstore's test helper (see index_version_test.go).
// Not exported so each secondary store test package owns its own.
type discardLogger struct{}

var _ logging.Logger = discardLogger{}

func (discardLogger) Tracef(string, ...any)                        {}
func (discardLogger) Debugf(string, ...any)                        {}
func (discardLogger) Infof(string, ...any)                         {}
func (discardLogger) Errorf(string, ...any)                        {}
func (discardLogger) Trace(...any)                                 {}
func (discardLogger) Debug(...any)                                 {}
func (discardLogger) Info(...any)                                  {}
func (discardLogger) Error(...any)                                 {}
func (l discardLogger) WithFields(map[string]any) logging.Logger   { return l }
func (l discardLogger) WithField(string, any) logging.Logger       { return l }
func (l discardLogger) WithContext(context.Context) logging.Logger { return l }
func (discardLogger) Writer() io.Writer                            { return io.Discard }
func (discardLogger) Enabled(logging.Level) bool                   { return false }
