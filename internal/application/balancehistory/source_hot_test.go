package balancehistory

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type hotSourceFixture struct {
	entry *auditpb.AuditEntry
	items []*auditpb.AuditItem
	logs  []*commonpb.Log
}

func newHotSourceTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	store, err := dal.NewStore(
		t.TempDir(),
		logging.FromContext(ctx),
		noop.NewMeterProvider().Meter("balance-history-source-test"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func hotAuditKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(sequence).
		Build()
}

func hotAuditItemKey(auditSequence uint64, orderIndex uint32) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
		PutUint64(auditSequence).
		PutUint32(orderIndex).
		Build()
}

func hotLogKey(sequence uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(sequence).
		Build()
}

func seedHotSource(t *testing.T, store *dal.Store, fixtures ...hotSourceFixture) {
	t.Helper()

	batch := store.OpenWriteSession()
	for _, fixture := range fixtures {
		require.NoError(t, batch.SetProto(hotAuditKey(fixture.entry.GetSequence()), fixture.entry))
		for _, item := range fixture.items {
			require.NoError(t, batch.SetProto(
				hotAuditItemKey(fixture.entry.GetSequence(), item.GetOrderIndex()),
				item,
			))
		}
		for _, log := range fixture.logs {
			require.NoError(t, batch.SetProto(hotLogKey(log.GetSequence()), log))
		}
	}
	require.NoError(t, batch.Commit())
}

func successfulHotFixture(auditSequence uint64, logSequences ...uint64) hotSourceFixture {
	items := make([]*auditpb.AuditItem, len(logSequences))
	logs := make([]*commonpb.Log, 0, len(logSequences))
	var minLogSequence, maxLogSequence uint64
	for index, logSequence := range logSequences {
		items[index] = &auditpb.AuditItem{
			OrderIndex:      uint32(index),
			SerializedOrder: []byte{byte(auditSequence), byte(index)},
			LogSequence:     logSequence,
		}
		if logSequence == 0 {
			continue
		}

		logs = append(logs, &commonpb.Log{Sequence: logSequence})
		if minLogSequence == 0 || logSequence < minLogSequence {
			minLogSequence = logSequence
		}
		if logSequence > maxLogSequence {
			maxLogSequence = logSequence
		}
	}

	return hotSourceFixture{
		entry: &auditpb.AuditEntry{
			Sequence:   auditSequence,
			OrderCount: uint32(len(items)),
			Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: minLogSequence,
				MaxLogSequence: maxLogSequence,
			}},
		},
		items: items,
		logs:  logs,
	}
}

func TestHotSourceReadsCompleteVerifiedProposals(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	seedHotSource(t, store,
		successfulHotFixture(1, 1, 2),
		successfulHotFixture(2, 0),
	)
	source := NewHotSource(store)

	head, err := source.Head(context.Background())
	require.NoError(t, err)
	require.Equal(t, Position{AuditSequence: 2, LogSequence: 2}, head)

	batch, err := source.Read(context.Background(), Position{}, 10)
	require.NoError(t, err)
	require.Equal(t, head, batch.Head)
	require.Equal(t, head, batch.Next)
	require.Len(t, batch.Proposals, 2)

	first := batch.Proposals[0]
	require.Equal(t, uint64(1), first.Entry.GetSequence())
	require.Len(t, first.Items, 2)
	require.Len(t, first.Logs, 2)
	require.Equal(t, uint64(1), first.Logs[0].GetSequence())
	require.Equal(t, uint64(2), first.Logs[1].GetSequence())

	second := batch.Proposals[1]
	require.Equal(t, uint64(2), second.Entry.GetSequence())
	require.Len(t, second.Items, 1)
	require.Len(t, second.Logs, 1)
	require.Nil(t, second.Logs[0])
}

func TestHotSourceReadsFailedProposalWithoutLogs(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	fixture := hotSourceFixture{
		entry: &auditpb.AuditEntry{
			Sequence:   1,
			OrderCount: 2,
			Outcome:    &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		},
		items: []*auditpb.AuditItem{
			{OrderIndex: 0, SerializedOrder: []byte{1}},
			{OrderIndex: 1, SerializedOrder: []byte{2}},
		},
	}
	seedHotSource(t, store, fixture)

	batch, err := NewHotSource(store).Read(context.Background(), Position{}, 1)
	require.NoError(t, err)
	require.Equal(t, Position{AuditSequence: 1}, batch.Next)
	require.Len(t, batch.Proposals, 1)
	require.Len(t, batch.Proposals[0].Logs, 2)
	require.Nil(t, batch.Proposals[0].Logs[0])
	require.Nil(t, batch.Proposals[0].Logs[1])
}

func TestHotSourceRejectsAuditGap(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	seedHotSource(t, store, successfulHotFixture(3, 0))

	batch, err := NewHotSource(store).Read(
		context.Background(),
		Position{AuditSequence: 1},
		10,
	)
	require.Error(t, err)
	require.Empty(t, batch.Proposals)

	var missing *ErrSourceMissing
	require.True(t, errors.As(err, &missing))
	require.ErrorContains(t, err, "expected audit sequence 2, first available sequence is 3")
}

func TestHotSourceRejectsMissingItemOrLog(t *testing.T) {
	t.Parallel()

	t.Run("missing item", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := successfulHotFixture(1, 0, 0)
		fixture.items = fixture.items[:1]
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var missing *ErrSourceMissing
		require.ErrorAs(t, err, &missing)
		require.ErrorContains(t, err, "declares 2 items but 1 are available")
	})

	t.Run("missing referenced log", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := successfulHotFixture(1, 1)
		fixture.logs = nil
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var missing *ErrSourceMissing
		require.ErrorAs(t, err, &missing)
		require.ErrorContains(t, err, "references missing log 1")
	})
}

func TestHotSourceLimitDoesNotSplitProposal(t *testing.T) {
	t.Parallel()

	store := newHotSourceTestStore(t)
	seedHotSource(t, store,
		successfulHotFixture(1, 1, 2, 3),
		successfulHotFixture(2, 4),
	)
	source := NewHotSource(store)

	first, err := source.Read(context.Background(), Position{}, 1)
	require.NoError(t, err)
	require.Equal(t, Position{AuditSequence: 1, LogSequence: 3}, first.Next)
	require.Equal(t, Position{AuditSequence: 2, LogSequence: 4}, first.Head)
	require.Len(t, first.Proposals, 1)
	require.Len(t, first.Proposals[0].Items, 3)
	require.Len(t, first.Proposals[0].Logs, 3)

	second, err := source.Read(context.Background(), first.Next, 1)
	require.NoError(t, err)
	require.Equal(t, second.Head, second.Next)
	require.Len(t, second.Proposals, 1)
	require.Equal(t, uint64(2), second.Proposals[0].Entry.GetSequence())
}

func TestHotSourceValidatesFreshLogCoverage(t *testing.T) {
	t.Parallel()

	t.Run("old reference below fresh range is allowed", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		first := successfulHotFixture(1, 1, 2, 3, 4, 5)
		second := hotSourceFixture{
			entry: &auditpb.AuditEntry{
				Sequence:   2,
				OrderCount: 2,
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
					MinLogSequence: 6,
					MaxLogSequence: 6,
				}},
			},
			items: []*auditpb.AuditItem{
				{OrderIndex: 0, SerializedOrder: []byte("old"), LogSequence: 3},
				{OrderIndex: 1, SerializedOrder: []byte("fresh"), LogSequence: 6},
			},
			logs: []*commonpb.Log{{Sequence: 6}},
		}
		seedHotSource(t, store, first, second)

		batch, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		require.NoError(t, err)
		require.Equal(t, uint64(6), batch.Next.LogSequence)
		require.Len(t, batch.Proposals, 2)
		require.Equal(t, uint64(3), batch.Proposals[1].Logs[0].GetSequence())
		require.Equal(t, uint64(6), batch.Proposals[1].Logs[1].GetSequence())
	})

	t.Run("fresh range is missing a sequence", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := hotSourceFixture{
			entry: &auditpb.AuditEntry{
				Sequence:   1,
				OrderCount: 1,
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
					MinLogSequence: 1,
					MaxLogSequence: 2,
				}},
			},
			items: []*auditpb.AuditItem{{OrderIndex: 0, LogSequence: 1}},
			logs:  []*commonpb.Log{{Sequence: 1}},
		}
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var missing *ErrSourceMissing
		require.ErrorAs(t, err, &missing)
		require.ErrorContains(t, err, "cannot fit")
	})

	t.Run("fresh sequence is referenced twice", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := hotSourceFixture{
			entry: &auditpb.AuditEntry{
				Sequence:   1,
				OrderCount: 2,
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
					MinLogSequence: 1,
					MaxLogSequence: 2,
				}},
			},
			items: []*auditpb.AuditItem{
				{OrderIndex: 0, LogSequence: 1},
				{OrderIndex: 1, LogSequence: 1},
			},
			logs: []*commonpb.Log{{Sequence: 1}},
		}
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var invalid *ErrSourceInvalid
		require.ErrorAs(t, err, &invalid)
		require.ErrorContains(t, err, "more than once")
	})

	t.Run("reference exceeds fresh range", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := hotSourceFixture{
			entry: &auditpb.AuditEntry{
				Sequence:   1,
				OrderCount: 1,
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
					MinLogSequence: 1,
					MaxLogSequence: 1,
				}},
			},
			items: []*auditpb.AuditItem{{OrderIndex: 0, LogSequence: 2}},
			logs:  []*commonpb.Log{{Sequence: 2}},
		}
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var invalid *ErrSourceInvalid
		require.ErrorAs(t, err, &invalid)
		require.ErrorContains(t, err, "beyond fresh range maximum")
	})

	t.Run("zero endpoints must appear together", func(t *testing.T) {
		t.Parallel()

		store := newHotSourceTestStore(t)
		fixture := successfulHotFixture(1, 0)
		fixture.entry.GetSuccess().MinLogSequence = 1
		seedHotSource(t, store, fixture)

		_, err := NewHotSource(store).Read(context.Background(), Position{}, 10)
		var invalid *ErrSourceInvalid
		require.ErrorAs(t, err, &invalid)
		require.ErrorContains(t, err, "partial fresh log range")
	})
}
