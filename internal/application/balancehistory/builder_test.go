package balancehistory

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const builderTestClusterID = "balance-history-builder-test"

func newBuilderTestHistoryStore(t *testing.T) *balancehistorystore.Store {
	t.Helper()

	store, err := balancehistorystore.New(t.TempDir(), logging.NopZap(), balancehistorystore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func newBuilderForTest(
	t *testing.T,
	source Source,
	store *balancehistorystore.Store,
	notifications *signal.Notifications,
) *Builder {
	t.Helper()

	return newBuilderForTestWithBackfillYield(t, source, store, notifications, time.Nanosecond)
}

func newBuilderForTestWithBackfillYield(
	t *testing.T,
	source Source,
	store *balancehistorystore.Store,
	notifications *signal.Notifications,
	backfillYield time.Duration,
) *Builder {
	t.Helper()

	return newBuilderForTestWithIntervals(
		t,
		source,
		store,
		notifications,
		backfillYield,
		DefaultDurabilityInterval,
	)
}

func newBuilderForTestWithIntervals(
	t *testing.T,
	source Source,
	store *balancehistorystore.Store,
	notifications *signal.Notifications,
	backfillYield time.Duration,
	durabilityInterval time.Duration,
) *Builder {
	t.Helper()

	return NewBuilder(
		source,
		store,
		notifications,
		builderTestClusterID,
		logging.NopZap(),
		noop.NewMeterProvider().Meter("balance-history-builder-test"),
		1,
		balancehistorystore.DefaultSegmentCompactionThreshold,
		backfillYield,
		durabilityInterval,
	)
}

func builderSuccessfulFixture(
	t *testing.T,
	auditSequence uint64,
	previousHash []byte,
	marker string,
	logs ...*commonpb.Log,
) hotSourceFixture {
	t.Helper()

	items := make([]*auditpb.AuditItem, len(logs))
	storedLogs := make([]*commonpb.Log, 0, len(logs))
	var minLogSequence, maxLogSequence uint64
	for index, log := range logs {
		item := &auditpb.AuditItem{
			OrderIndex:      uint32(index),
			SerializedOrder: []byte(marker + string(rune(index))),
		}
		if log != nil {
			item.LogSequence = log.GetSequence()
			storedLogs = append(storedLogs, log)
			if minLogSequence == 0 || log.GetSequence() < minLogSequence {
				minLogSequence = log.GetSequence()
			}
			if log.GetSequence() > maxLogSequence {
				maxLogSequence = log.GetSequence()
			}
		}
		items[index] = item
	}

	entry := &auditpb.AuditEntry{
		Sequence:   auditSequence,
		Timestamp:  &commonpb.Timestamp{Data: auditSequence},
		ProposalId: auditSequence,
		OrderCount: uint32(len(items)),
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: minLogSequence,
			MaxLogSequence: maxLogSequence,
		}},
	}
	entry.Hash = builderAuditHash(t, entry, items, previousHash)

	return hotSourceFixture{entry: entry, items: items, logs: storedLogs}
}

func builderAuditHash(
	t *testing.T,
	entry *auditpb.AuditEntry,
	items []*auditpb.AuditItem,
	previousHash []byte,
) []byte {
	t.Helper()

	header, err := state.BuildHashedHeaderPayload(entry)
	require.NoError(t, err)
	slices := make([][]byte, 0, len(items)+1)
	slices = append(slices, header)
	for _, item := range items {
		slices = append(slices, state.BuildPerItemPayload(item))
	}
	_, hash := processing.NewHashGenerator(
		commonpb.HashAlgorithm(entry.GetHashVersion()),
		builderTestClusterID,
	).Compute(nil, previousHash, slices)

	return append([]byte(nil), hash...)
}

func builderCreateLedgerLog(sequence uint64, name string, id uint32) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: name, Id: id},
		}},
	}
}

func builderTransactionLog(sequence uint64, ledger string, amount uint64) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log: &commonpb.LedgerLog{
				Id: sequence,
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
						Postings: []*commonpb.Posting{{
							Source: "world", Destination: "cash", Asset: "USD",
							Amount: &commonpb.Uint256{V0: amount},
						}},
						Timestamp:  &commonpb.Timestamp{Data: 100},
						InsertedAt: &commonpb.Timestamp{Data: 200},
					}},
				}},
			},
		}}},
	}
}

func builderHistoricalBalancesConfigLog(sequence uint64, ledger string, enabled bool) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log: &commonpb.LedgerLog{
				Id: sequence,
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_ConfiguredHistoricalBalances{
					ConfiguredHistoricalBalances: &commonpb.ConfiguredHistoricalBalancesLog{Enabled: enabled},
				}},
			},
		}}},
	}
}

func TestFreshLogRangeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		entry     *auditpb.AuditEntry
		watermark uint64
		wantMin   uint64
		wantMax   uint64
		wantErr   string
	}{
		{
			name:  "failure has no fresh range",
			entry: &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}}},
		},
		{name: "missing outcome", entry: &auditpb.AuditEntry{Sequence: 1}, wantErr: "has no outcome"},
		{
			name: "partial range",
			entry: &auditpb.AuditEntry{Sequence: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: 1,
			}}},
			wantErr: "partial fresh log range",
		},
		{
			name: "descending range",
			entry: &auditpb.AuditEntry{Sequence: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: 2,
				MaxLogSequence: 1,
			}}},
			wantErr: "descending fresh log range",
		},
		{
			name: "discontinuous range",
			entry: &auditpb.AuditEntry{Sequence: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: 3,
				MaxLogSequence: 4,
			}}},
			watermark: 1,
			wantErr:   "starts fresh log range at 3 after watermark 1",
		},
		{
			name: "valid range",
			entry: &auditpb.AuditEntry{Sequence: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
				MinLogSequence: 3,
				MaxLogSequence: 4,
			}}},
			watermark: 2,
			wantMin:   3,
			wantMax:   4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			minimum, maximum, err := freshLogRange(test.entry, test.watermark)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)

				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantMin, minimum)
			require.Equal(t, test.wantMax, maximum)
		})
	}
}

func TestAuditHashSlicesRejectsUnboundOrMissingData(t *testing.T) {
	t.Parallel()

	entry := &auditpb.AuditEntry{
		Sequence: 1,
		Outcome:  &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
	}
	entry.Items = []*auditpb.AuditItem{{OrderIndex: 0}}
	_, err := auditHashSlices(entry, nil)
	require.ErrorContains(t, err, "embeds 1 unbound items")

	entry.Items = nil
	entry.Outcome = nil
	_, err = auditHashSlices(entry, nil)
	require.ErrorContains(t, err, "cannot rebuild its hash header")

	entry.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}}
	_, err = auditHashSlices(entry, []*auditpb.AuditItem{nil})
	require.ErrorContains(t, err, "is missing item 0")

	slices, err := auditHashSlices(entry, []*auditpb.AuditItem{{OrderIndex: 0}})
	require.NoError(t, err)
	require.Len(t, slices, 2)
}

func TestReducerFromManifestValidation(t *testing.T) {
	t.Parallel()

	_, err := reducerFromManifest(balancehistorystore.Manifest{
		AuditWatermark: 1,
		LogWatermark:   1,
		ReducerState: domainhistory.State{
			Seen: []domainhistory.IncarnationState{{Name: "", ID: 1}},
		},
	})
	require.ErrorContains(t, err, "restoring reducer at audit 1")

	_, err = reducerFromManifest(balancehistorystore.Manifest{
		AuditWatermark: 1,
		LogWatermark:   1,
		ReducerState: domainhistory.State{
			HasLast: true,
			Last:    domainhistory.Position{AuditSequence: 2, LogSequence: 2},
		},
	})
	require.ErrorContains(t, err, "exceeds manifest watermarks")
}

func TestReduceVerifiedBatchRejectsMalformedBatches(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		configure  func(t *testing.T, proposal *VerifiedProposal)
		rehash     bool
		alterBatch func(batch *Batch)
		want       string
	}
	tests := []testCase{
		{
			name: "missing entry",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Entry = nil
			},
			want: "has audit sequence 0, want 1",
		},
		{
			name: "item and log count mismatch",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Logs = nil
			},
			want: "has 1 items, 0 logs",
		},
		{
			name: "audit hash mismatch",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Entry.Hash = []byte("tampered")
			},
			want: "audit hash chain mismatch",
		},
		{
			name: "invalid item order",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Items[0].OrderIndex = 2
			},
			rehash: true,
			want:   "invalid item at position 0",
		},
		{
			name: "failed proposal references log",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Entry.Outcome = &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}}
			},
			rehash: true,
			want:   "failed audit sequence 1 item 0 references log 1",
		},
		{
			name: "reference with empty fresh range",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Entry.GetSuccess().MinLogSequence = 0
				proposal.Entry.GetSuccess().MaxLogSequence = 0
			},
			rehash: true,
			want:   "references log 1 with an empty fresh range",
		},
		{
			name: "reference beyond maximum",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Items[0].LogSequence = 2
			},
			rehash: true,
			want:   "beyond fresh range maximum 1",
		},
		{
			name: "payload without sequence",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Items[0].LogSequence = 0
				proposal.Entry.GetSuccess().MinLogSequence = 0
				proposal.Entry.GetSuccess().MaxLogSequence = 0
			},
			rehash: true,
			want:   "has a log payload without a log sequence",
		},
		{
			name: "missing fresh log payload",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Logs[0] = nil
			},
			want: "fresh log 1 is missing",
		},
		{
			name: "reducer rejects malformed log",
			configure: func(_ *testing.T, proposal *VerifiedProposal) {
				proposal.Logs[0] = &commonpb.Log{Sequence: 1}
			},
			want: "reducing audit sequence 1 item 0 log 1",
		},
		{
			name:      "source next position mismatch",
			configure: func(_ *testing.T, _ *VerifiedProposal) {},
			alterBatch: func(batch *Batch) {
				batch.Next.LogSequence = 0
			},
			want: "source next position is (1,0), verified position is (1,1)",
		},
		{
			name:      "source next hash mismatch",
			configure: func(_ *testing.T, _ *VerifiedProposal) {},
			alterBatch: func(batch *Batch) {
				batch.Next.AuditHash = []byte("wrong")
			},
			want: "source next hash",
		},
		{
			name:      "source head behind next",
			configure: func(_ *testing.T, _ *VerifiedProposal) {},
			alterBatch: func(batch *Batch) {
				batch.Head = Position{}
			},
			want: "source batch head is behind",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := builderSuccessfulFixture(
				t,
				1,
				nil,
				"test",
				builderCreateLedgerLog(1, "default", 7),
			)
			proposal := VerifiedProposal{
				Entry: fixture.entry.CloneVT(),
				Items: []*auditpb.AuditItem{fixture.items[0].CloneVT()},
				Logs:  []*commonpb.Log{fixture.logs[0].CloneVT()},
			}
			test.configure(t, &proposal)
			if test.rehash {
				proposal.Entry.Hash = builderAuditHash(t, proposal.Entry, proposal.Items, nil)
			}

			next := Position{AuditSequence: 1, LogSequence: 1}
			if proposal.Entry != nil {
				next.AuditHash = append([]byte(nil), proposal.Entry.GetHash()...)
				if proposal.Entry.GetFailure() != nil || proposal.Entry.GetSuccess().GetMaxLogSequence() == 0 {
					next.LogSequence = 0
				}
			}
			batch := Batch{Proposals: []VerifiedProposal{proposal}, Next: next, Head: clonePosition(next)}
			if test.alterBatch != nil {
				test.alterBatch(&batch)
			}

			_, _, _, err := reduceVerifiedBatch(
				builderTestClusterID,
				domainhistory.NewReducer(),
				Position{},
				batch,
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestBuilderBuildsGenesisAndMonetaryEffects(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	configured := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "configure", builderHistoricalBalancesConfigLog(2, "default", true))
	transaction := builderSuccessfulFixture(t, 3, configured.entry.GetHash(), "tx", builderTransactionLog(3, "default", 42))
	seedHotSource(t, primary, created, configured, transaction)

	history := newBuilderTestHistoryStore(t)
	_, err := history.OpenView(3)
	var building *balancehistorystore.ErrBuilding
	require.ErrorAs(t, err, &building)

	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
	require.NoError(t, builder.boot(context.Background()))

	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(3), manifest.AuditWatermark)
	require.Equal(t, uint64(3), manifest.LogWatermark)
	require.Equal(t, transaction.entry.GetHash(), manifest.AuditHash)
	require.True(t, manifest.SourceComplete)
	require.True(t, manifest.ReducerState.HasLast)
	require.Equal(t, uint64(3), builder.LastProcessedAuditSequence())
	require.Equal(t, uint64(3), builder.SourceHeadAuditSequence())

	view, err := history.OpenView(3)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	volumes, err := view.ReadVolumes("default", balancehistorystore.TemporalityEffective, 100, []string{"cash"})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, big.NewInt(42), volumes[0].Input)
	require.Zero(t, volumes[0].Output.Sign())
}

func TestBuilderReconcilesEmptyAndPartialStores(t *testing.T) {
	t.Parallel()

	t.Run("empty authoritative source becomes ready", func(t *testing.T) {
		t.Parallel()

		primary := newHotSourceTestStore(t)
		history := newBuilderTestHistoryStore(t)
		builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
		require.NoError(t, builder.boot(context.Background()))
		require.True(t, builder.Ready())

		manifest, err := history.Manifest()
		require.NoError(t, err)
		require.True(t, manifest.SourceComplete)
		require.Zero(t, manifest.AuditWatermark)
		require.Zero(t, manifest.LogWatermark)
		require.Empty(t, manifest.Segments)

		view, err := history.OpenView(0)
		require.NoError(t, err)
		require.NoError(t, view.Close())
	})

	t.Run("partial projection is reset to the authoritative empty source", func(t *testing.T) {
		t.Parallel()

		primary := newHotSourceTestStore(t)
		history := newBuilderTestHistoryStore(t)
		_, err := history.Publish(balancehistorystore.Publication{
			Coverage: balancehistorystore.Coverage{
				AuditSequence: 1,
				LogSequence:   1,
				AuditHash:     []byte{1},
			},
		})
		require.NoError(t, err)
		builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
		require.NoError(t, builder.boot(context.Background()))
		require.True(t, builder.Ready())

		manifest, err := history.Manifest()
		require.NoError(t, err)
		require.True(t, manifest.SourceComplete)
		require.Zero(t, manifest.AuditWatermark)
		require.Zero(t, manifest.LogWatermark)
		require.Empty(t, manifest.Segments)

		view, err := history.OpenView(0)
		require.NoError(t, err)
		require.NoError(t, view.Close())
	})
}

func TestBuilderAdvancesAcrossNoLogProposal(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	noLog := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "no-log", nil)
	seedHotSource(t, primary, created, noLog)

	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, NewHotSource(primary), history, nil).boot(context.Background()))

	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.AuditWatermark)
	require.Equal(t, uint64(1), manifest.LogWatermark)
	require.Equal(t, noLog.entry.GetHash(), manifest.AuditHash)
	require.Empty(t, manifest.Segments)
}

func TestBuilderDoesNotReduceHistoricalLogReferencesTwice(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	configured := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "configure", builderHistoricalBalancesConfigLog(2, "default", true))
	firstTransaction := builderSuccessfulFixture(t, 3, configured.entry.GetHash(), "first", builderTransactionLog(3, "default", 5))
	secondLog := builderTransactionLog(4, "default", 7)
	items := []*auditpb.AuditItem{
		{OrderIndex: 0, SerializedOrder: []byte("old-reference"), LogSequence: 3},
		{OrderIndex: 1, SerializedOrder: []byte("fresh-log"), LogSequence: 4},
	}
	entry := &auditpb.AuditEntry{
		Sequence:   4,
		Timestamp:  &commonpb.Timestamp{Data: 4},
		ProposalId: 4,
		OrderCount: 2,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: 4,
			MaxLogSequence: 4,
		}},
	}
	entry.Hash = builderAuditHash(t, entry, items, firstTransaction.entry.GetHash())
	mixed := hotSourceFixture{entry: entry, items: items, logs: []*commonpb.Log{secondLog}}
	seedHotSource(t, primary, created, configured, firstTransaction, mixed)

	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, NewHotSource(primary), history, nil).boot(context.Background()))

	view, err := history.OpenView(4)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	volumes, err := view.ReadVolumes("default", balancehistorystore.TemporalityEffective, 100, []string{"cash"})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, "12", volumes[0].Input.String())
}

func TestBuilderRestartsFromPersistedReducerState(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	configured := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "configure", builderHistoricalBalancesConfigLog(2, "default", true))
	seedHotSource(t, primary, created, configured)

	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, NewHotSource(primary), history, nil).boot(context.Background()))

	transaction := builderSuccessfulFixture(t, 3, configured.entry.GetHash(), "tx", builderTransactionLog(3, "default", 11))
	seedHotSource(t, primary, transaction)
	restarted := newBuilderForTest(t, NewHotSource(primary), history, nil)
	require.NoError(t, restarted.boot(context.Background()))

	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(3), manifest.AuditWatermark)
	require.Equal(t, transaction.entry.GetHash(), manifest.AuditHash)
	view, err := history.OpenView(3)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	volumes, err := view.ReadVolumes("default", balancehistorystore.TemporalityInsertion, 200, []string{"cash"})
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, "11", volumes[0].Input.String())
}

func TestBuilderFailsClosedOnCorruptedAuditHash(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	fixture := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	fixture.entry.Hash[0] ^= 0xff
	seedHotSource(t, primary, fixture)

	history := newBuilderTestHistoryStore(t)
	require.NoError(t, newBuilderForTest(t, NewHotSource(primary), history, nil).boot(context.Background()))

	_, err := history.OpenView(1)
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "audit hash chain mismatch")
}

func TestBuilderQuarantinedBootRebuildsAndBecomesReady(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	fixture := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	seedHotSource(t, primary, fixture)
	history := newBuilderTestHistoryStore(t)
	require.NoError(t, history.Quarantine("pre-existing verifier failure"))
	_, err := history.OpenView(1)
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)

	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
	require.NoError(t, builder.boot(context.Background()))
	require.Equal(t, uint64(1), builder.LastProcessedAuditSequence())

	view, err := history.OpenView(1)
	require.NoError(t, err)
	require.NoError(t, view.Close())
	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.True(t, manifest.SourceComplete)
	require.Equal(t, fixture.entry.GetHash(), manifest.AuditHash)
}

func TestBuilderFailedCorruptionRebuildRemainsQuarantined(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	fixture := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	fixture.entry.Hash[0] ^= 0xff
	seedHotSource(t, primary, fixture)
	history := newBuilderTestHistoryStore(t)
	require.NoError(t, history.Quarantine("pre-existing verifier failure"))

	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
	require.NoError(t, builder.boot(context.Background()))
	require.Zero(t, builder.LastProcessedAuditSequence())

	_, err := history.OpenView(1)
	var quarantined *balancehistorystore.ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	require.ErrorContains(t, err, "audit hash chain mismatch")
	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.Version)
	require.False(t, manifest.SourceComplete)
}

func TestBuilderFailsClosedOnMissingSourcePrefix(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	first := builderSuccessfulFixture(t, 1, nil, "first", builderCreateLedgerLog(1, "default", 7))
	second := builderSuccessfulFixture(t, 2, first.entry.GetHash(), "second", builderTransactionLog(2, "default", 3))
	seedHotSource(t, primary, second)

	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
	require.NoError(t, builder.boot(context.Background()))

	_, err := history.OpenView(2)
	var missing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	require.ErrorContains(t, err, "expected audit sequence 1")
}

func TestBuilderResetsAndRebuildsAfterPrimaryRollback(t *testing.T) {
	t.Parallel()

	t.Run("head behind manifest", func(t *testing.T) {
		t.Parallel()

		oldPrimary := newHotSourceTestStore(t)
		created := builderSuccessfulFixture(t, 1, nil, "old-create", builderCreateLedgerLog(1, "default", 7))
		transaction := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "old-tx", builderTransactionLog(2, "default", 5))
		seedHotSource(t, oldPrimary, created, transaction)
		history := newBuilderTestHistoryStore(t)
		require.NoError(t, newBuilderForTest(t, NewHotSource(oldPrimary), history, nil).boot(context.Background()))

		rolledBackPrimary := newHotSourceTestStore(t)
		seedHotSource(t, rolledBackPrimary, created)
		require.NoError(t, newBuilderForTest(t, NewHotSource(rolledBackPrimary), history, nil).boot(context.Background()))

		manifest, err := history.Manifest()
		require.NoError(t, err)
		require.Equal(t, uint64(1), manifest.AuditWatermark)
		require.Equal(t, uint64(1), manifest.LogWatermark)
		require.Equal(t, created.entry.GetHash(), manifest.AuditHash)
		require.Empty(t, manifest.Segments)
	})

	t.Run("same sequence divergent hash", func(t *testing.T) {
		t.Parallel()

		oldPrimary := newHotSourceTestStore(t)
		oldCreated := builderSuccessfulFixture(t, 1, nil, "old", builderCreateLedgerLog(1, "default", 7))
		seedHotSource(t, oldPrimary, oldCreated)
		history := newBuilderTestHistoryStore(t)
		require.NoError(t, newBuilderForTest(t, NewHotSource(oldPrimary), history, nil).boot(context.Background()))

		newPrimary := newHotSourceTestStore(t)
		newCreated := builderSuccessfulFixture(t, 1, nil, "new", builderCreateLedgerLog(1, "replacement", 9))
		require.NotEqual(t, oldCreated.entry.GetHash(), newCreated.entry.GetHash())
		seedHotSource(t, newPrimary, newCreated)
		require.NoError(t, newBuilderForTest(t, NewHotSource(newPrimary), history, nil).boot(context.Background()))

		manifest, err := history.Manifest()
		require.NoError(t, err)
		require.Equal(t, newCreated.entry.GetHash(), manifest.AuditHash)
		require.Equal(t, "replacement", manifest.ReducerState.Active[0].Name)
		require.Equal(t, uint32(9), manifest.ReducerState.Active[0].ID)
	})
}

func TestBuilderReadyRequiresFreshSourceReconciliationAfterRestart(t *testing.T) {
	t.Parallel()

	oldPrimary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	transaction := builderSuccessfulFixture(
		t,
		2,
		created.entry.GetHash(),
		"transaction",
		builderTransactionLog(2, "default", 9),
	)
	seedHotSource(t, oldPrimary, created, transaction)
	history := newBuilderTestHistoryStore(t)
	seedBuilder := newBuilderForTest(t, NewHotSource(oldPrimary), history, nil)
	require.NoError(t, seedBuilder.boot(context.Background()))
	require.True(t, seedBuilder.Ready())
	view, err := history.OpenView(2)
	require.NoError(t, err)
	require.NoError(t, view.Close())

	lowerPrimary := newHotSourceTestStore(t)
	seedHotSource(t, lowerPrimary, created)
	lowerSource := NewHotSource(lowerPrimary)
	wantHeadErr := errors.New("injected source head outage")
	headStarted := make(chan struct{})
	releaseHead := make(chan struct{})
	lowerHeadStarted := make(chan struct{})
	releaseLowerHead := make(chan struct{})
	headCalls := 0
	source := NewMockSource(gomock.NewController(t))
	source.EXPECT().Head(gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context) (Position, error) {
		headCalls++
		switch headCalls {
		case 1:
			close(headStarted)
			select {
			case <-ctx.Done():
				return Position{}, ctx.Err()
			case <-releaseHead:
				return Position{}, wantHeadErr
			}
		case 2:
			close(lowerHeadStarted)
			select {
			case <-ctx.Done():
				return Position{}, ctx.Err()
			case <-releaseLowerHead:
			}
		}

		return lowerSource.Head(ctx)
	})
	source.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(lowerSource.Read)
	builder := newBuilderForTest(t, source, history, nil)
	builder.ready.Store(true)
	builder.Start()
	t.Cleanup(func() { _ = builder.Stop() })

	select {
	case <-headStarted:
	case <-time.After(time.Second):
		t.Fatal("builder did not attempt to read the source head")
	}
	// Start must invalidate any process-local state before the source answers.
	require.False(t, builder.Ready())
	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.AuditWatermark)

	close(releaseHead)
	select {
	case <-lowerHeadStarted:
	case <-time.After(time.Second):
		t.Fatal("builder did not retry the source head after the boot error")
	}
	require.False(t, builder.Ready())
	manifest, err = history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(2), manifest.AuditWatermark)

	close(releaseLowerHead)
	require.Eventually(t, func() bool {
		manifest, err = history.Manifest()

		return err == nil && builder.Ready() && manifest.AuditWatermark == 1
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, created.entry.GetHash(), manifest.AuditHash)
	require.NoError(t, builder.Stop())
	require.False(t, builder.Ready())
}

func TestBuilderStartRetriesAfterSourceRepair(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	first := builderSuccessfulFixture(t, 1, nil, "first", builderCreateLedgerLog(1, "default", 7))
	second := builderSuccessfulFixture(t, 2, first.entry.GetHash(), "second", builderTransactionLog(2, "default", 9))
	seedHotSource(t, primary, second)
	history := newBuilderTestHistoryStore(t)
	notifications := signal.NewNotifications()
	builder := newBuilderForTest(t, NewHotSource(primary), history, notifications)
	builder.Start()
	t.Cleanup(func() { require.NoError(t, builder.Stop()) })

	require.Eventually(t, func() bool {
		_, err := history.OpenView(2)
		var missing *balancehistorystore.ErrSourceMissing

		return errors.As(err, &missing)
	}, 5*time.Second, 10*time.Millisecond)

	seedHotSource(t, primary, first)
	notifications.NotifyLogsCommitted(2)
	require.Eventually(t, func() bool {
		if builder.LastProcessedAuditSequence() != 2 {
			return false
		}
		view, err := history.OpenView(2)
		if err != nil {
			return false
		}

		return view.Close() == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func TestBuilderBootYieldsBetweenBatchesAndHonorsCancellation(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	first := builderSuccessfulFixture(t, 1, nil, "first", builderCreateLedgerLog(1, "default", 7))
	second := builderSuccessfulFixture(t, 2, first.entry.GetHash(), "second", builderTransactionLog(2, "default", 9))
	seedHotSource(t, primary, first, second)
	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTestWithBackfillYield(t, NewHotSource(primary), history, nil, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- builder.boot(ctx)
	}()

	require.Eventually(t, func() bool {
		return builder.LastProcessedAuditSequence() == 1
	}, 5*time.Second, 10*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("boot did not leave the backfill yield after cancellation")
	}
	require.Equal(t, uint64(1), builder.LastProcessedAuditSequence())
	manifest, err := history.Manifest()
	require.NoError(t, err)
	require.Equal(t, uint64(1), manifest.AuditWatermark)
}

func TestBuilderDurabilityCadenceRetriesWithoutWaitingForAnotherInterval(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	seedHotSource(t, primary, created)
	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTestWithIntervals(t, NewHotSource(primary), history, nil, time.Nanosecond, time.Hour)

	now := time.Unix(2_000_000_000, 0)
	builder.durabilityNow = func() time.Time { return now }
	builder.lastDurabilitySync = now
	syncCalls := 0
	builder.durabilitySync = func() error {
		syncCalls++

		return nil
	}

	require.NoError(t, builder.tick(context.Background()))
	require.Equal(t, 1, syncCalls, "the first successful source reconciliation forces durability")
	require.Equal(t, uint64(1), builder.LastProcessedAuditSequence())
	require.Equal(t, uint64(1), builder.LastDurableAuditSequence())
	require.True(t, builder.Ready())

	now = now.Add(2 * time.Hour)
	require.NoError(t, builder.tick(context.Background()))
	require.Equal(t, 2, syncCalls)
	require.Equal(t, uint64(1), builder.LastDurableAuditSequence())

	transaction := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "tx", builderTransactionLog(2, "default", 9))
	seedHotSource(t, primary, transaction)
	now = now.Add(TickInterval)
	require.NoError(t, builder.tick(context.Background()))
	require.Equal(t, uint64(2), builder.LastProcessedAuditSequence())
	require.Equal(t, uint64(1), builder.LastDurableAuditSequence())

	wantSyncErr := errors.New("injected WAL barrier failure")
	failureAttempts := 0
	builder.durabilitySync = func() error {
		failureAttempts++
		if failureAttempts == 1 {
			return wantSyncErr
		}

		return nil
	}
	now = now.Add(2 * time.Hour)
	err := builder.tick(context.Background())
	require.ErrorIs(t, err, wantSyncErr)
	require.ErrorIs(t, builder.LastDurabilityError(), wantSyncErr)
	require.Equal(t, uint64(1), builder.durabilitySyncFailures.Load())
	require.Equal(t, uint64(1), builder.LastDurableAuditSequence())

	// The failed barrier does not advance lastDurabilitySync, so the next
	// worker tick retries immediately without waiting for another interval.
	now = now.Add(TickInterval)
	require.NoError(t, builder.tick(context.Background()))
	require.Equal(t, 2, failureAttempts)
	require.NoError(t, builder.LastDurabilityError())
	require.Equal(t, uint64(2), builder.LastDurableAuditSequence())
}

func TestBuilderTickProcessesCommitWithoutPollingDelay(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	seedHotSource(t, primary, created)
	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)
	now := time.Unix(2_000_000_000, 0)
	builder.durabilityNow = func() time.Time { return now }

	require.NoError(t, builder.boot(context.Background()))
	require.NoError(t, builder.tick(context.Background()), "idle fallback tick")

	transaction := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "tx", builderTransactionLog(2, "default", 9))
	seedHotSource(t, primary, transaction)
	require.NoError(t, builder.tick(context.Background()), "post-commit wake at the same clock instant")
	require.Equal(t, uint64(2), builder.LastProcessedAuditSequence())
}

func TestBuilderTickDrainsEveryVisibleBatch(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	first := builderSuccessfulFixture(t, 1, nil, "first", builderCreateLedgerLog(1, "default", 7))
	second := builderSuccessfulFixture(t, 2, first.entry.GetHash(), "second", builderTransactionLog(2, "default", 9))
	third := builderSuccessfulFixture(t, 3, second.entry.GetHash(), "third", builderTransactionLog(3, "default", 11))
	seedHotSource(t, primary, first, second, third)
	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTest(t, NewHotSource(primary), history, nil)

	require.NoError(t, builder.tick(context.Background()))
	require.Equal(t, uint64(3), builder.LastProcessedAuditSequence())
	require.True(t, builder.Ready())
}

func TestBuilderSteadyStateUsesReadSnapshotHeadWithoutSeparateProbe(t *testing.T) {
	t.Parallel()

	store := newBuilderTestHistoryStore(t)
	mockController := gomock.NewController(t)
	source := NewMockSource(mockController)
	source.EXPECT().
		Read(gomock.Any(), Position{}, 1).
		Return(Batch{Next: Position{}, Head: Position{}}, nil)
	builder := newBuilderForTest(t, source, store, nil)

	caughtUp, err := builder.processOnce(context.Background())
	require.NoError(t, err)
	require.True(t, caughtUp)
}

func TestBuilderBootSyncsPeriodicallyAndAtCaughtUpHead(t *testing.T) {
	t.Parallel()

	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	transaction := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "tx", builderTransactionLog(2, "default", 9))
	seedHotSource(t, primary, created, transaction)
	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTestWithIntervals(t, NewHotSource(primary), history, nil, time.Nanosecond, time.Hour)

	now := time.Unix(2_000_000_000, 0)
	builder.durabilityNow = func() time.Time { return now }
	builder.lastDurabilitySync = now.Add(-2 * time.Hour)
	syncCalls := 0
	builder.durabilitySync = func() error {
		syncCalls++

		return nil
	}

	require.NoError(t, builder.boot(context.Background()))
	require.Equal(t, 2, syncCalls, "one periodic barrier between batches plus one forced barrier at the pinned head")
	require.Equal(t, uint64(2), builder.LastDurableAuditSequence())
}

func TestBuilderStopRetriesFinalDurabilityBarrier(t *testing.T) {
	t.Parallel()

	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTest(t, nil, history, nil)
	builder.lastProcessedAuditSequence.Store(9)
	builder.ready.Store(true)
	wantSyncErr := errors.New("injected stop WAL barrier failure")
	attempts := 0
	builder.durabilitySync = func() error {
		attempts++
		if attempts == 1 {
			return wantSyncErr
		}

		return nil
	}

	err := builder.Stop()
	require.ErrorIs(t, err, wantSyncErr)
	require.False(t, builder.Ready())
	require.ErrorIs(t, builder.LastDurabilityError(), wantSyncErr)
	require.Zero(t, builder.LastDurableAuditSequence())

	builder.ready.Store(true)
	require.NoError(t, builder.Stop())
	require.False(t, builder.Ready())
	require.Equal(t, 2, attempts)
	require.NoError(t, builder.LastDurabilityError())
	require.Equal(t, uint64(9), builder.LastDurableAuditSequence())
}

func TestBuilderRepairRemainsFailClosedBeforePinnedHead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prepare    func(*testing.T, *balancehistorystore.Store)
		assertFail func(*testing.T, error)
	}{
		{
			name: "quarantined rebuild",
			prepare: func(t *testing.T, store *balancehistorystore.Store) {
				t.Helper()
				require.NoError(t, store.Quarantine("injected corruption"))
			},
			assertFail: func(t *testing.T, err error) {
				t.Helper()
				var quarantined *balancehistorystore.ErrQuarantined
				require.ErrorAs(t, err, &quarantined)
			},
		},
		{
			name: "missing-source repair",
			prepare: func(t *testing.T, store *balancehistorystore.Store) {
				t.Helper()
				require.NoError(t, store.MarkSourceMissing("injected source gap"))
			},
			assertFail: func(t *testing.T, err error) {
				t.Helper()
				var missing *balancehistorystore.ErrSourceMissing
				require.ErrorAs(t, err, &missing)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			primary := newHotSourceTestStore(t)
			created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
			transaction := builderSuccessfulFixture(t, 2, created.entry.GetHash(), "tx", builderTransactionLog(2, "default", 9))
			seedHotSource(t, primary, created, transaction)
			history := newBuilderTestHistoryStore(t)
			test.prepare(t, history)
			builder := newBuilderForTestWithBackfillYield(t, NewHotSource(primary), history, nil, time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- builder.boot(ctx)
			}()

			require.Eventually(t, func() bool {
				return builder.LastProcessedAuditSequence() == 1
			}, 5*time.Second, 10*time.Millisecond)
			_, err := history.OpenView(1)
			test.assertFail(t, err)

			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("builder did not leave the bounded backfill wait after cancellation")
			}
			require.Equal(t, uint64(1), builder.LastProcessedAuditSequence())
		})
	}
}

func TestBuilderDurabilityMetricsExposeRetryState(t *testing.T) {
	t.Parallel()

	history := newBuilderTestHistoryStore(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	builder := NewBuilder(
		nil,
		history,
		nil,
		builderTestClusterID,
		logging.NopZap(),
		provider.Meter("balance-history-builder-durability-test"),
		1,
		balancehistorystore.DefaultSegmentCompactionThreshold,
		time.Nanosecond,
		time.Hour,
	)
	registration, err := builder.registerDurabilityMetrics()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, registration.Unregister()) })

	builder.lastDurableAuditSequence.Store(7)
	builder.durabilitySyncFailures.Store(2)
	builder.durabilitySyncUnhealthy.Store(true)

	collect := func() map[string]int64 {
		var resources metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &resources))
		values := make(map[string]int64)
		for _, scope := range resources.ScopeMetrics {
			for _, instrument := range scope.Metrics {
				switch data := instrument.Data.(type) {
				case metricdata.Gauge[int64]:
					require.Len(t, data.DataPoints, 1)
					require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
					values[instrument.Name] = data.DataPoints[0].Value
				case metricdata.Sum[int64]:
					require.Len(t, data.DataPoints, 1)
					require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
					values[instrument.Name] = data.DataPoints[0].Value
				}
			}
		}

		return values
	}

	values := collect()
	require.Equal(t, int64(7), values["balancehistory.builder.last_durable_audit_sequence"])
	require.Equal(t, int64(2), values["balancehistory.builder.durability_sync_failures"])
	require.Equal(t, int64(1), values["balancehistory.builder.durability_sync_error"])

	builder.lastProcessedAuditSequence.Store(9)
	builder.durabilitySync = func() error { return nil }
	require.NoError(t, builder.syncDurability(true))
	values = collect()
	require.Equal(t, int64(9), values["balancehistory.builder.last_durable_audit_sequence"])
	require.Equal(t, int64(2), values["balancehistory.builder.durability_sync_failures"])
	require.Zero(t, values["balancehistory.builder.durability_sync_error"])
}

func TestBuilderRecordsBoundedPublishLagWithoutLabels(t *testing.T) {
	t.Parallel()

	publishedAt := time.Unix(2_000_000_000, 0).UTC()
	committedAt := publishedAt.Add(-1500 * time.Millisecond)
	primary := newHotSourceTestStore(t)
	created := builderSuccessfulFixture(t, 1, nil, "create", builderCreateLedgerLog(1, "default", 7))
	created.entry.Timestamp = &commonpb.Timestamp{Data: uint64(committedAt.UnixMicro())}
	created.entry.Hash = builderAuditHash(t, created.entry, created.items, nil)
	transaction := builderSuccessfulFixture(
		t,
		2,
		created.entry.GetHash(),
		"transaction",
		builderTransactionLog(2, "default", 42),
	)
	transaction.entry.Timestamp = &commonpb.Timestamp{Data: uint64(committedAt.UnixMicro())}
	transaction.entry.Hash = builderAuditHash(t, transaction.entry, transaction.items, created.entry.GetHash())
	seedHotSource(t, primary, created, transaction)
	history := newBuilderTestHistoryStore(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	builder := NewBuilder(
		NewHotSource(primary),
		history,
		nil,
		builderTestClusterID,
		logging.NopZap(),
		provider.Meter("balance-history-builder-lag-test"),
		2,
		balancehistorystore.DefaultSegmentCompactionThreshold,
		time.Nanosecond,
		time.Hour,
	)
	builder.durabilityNow = func() time.Time { return publishedAt }

	require.NoError(t, builder.boot(context.Background()))
	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resources))
	values := map[string]int64{}
	histograms := map[string]metricdata.HistogramDataPoint[int64]{}
	for _, scope := range resources.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			switch data := instrument.Data.(type) {
			case metricdata.Sum[int64]:
				require.Len(t, data.DataPoints, 1)
				require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
				values[instrument.Name] = data.DataPoints[0].Value
			case metricdata.Histogram[int64]:
				require.Len(t, data.DataPoints, 1)
				require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
				histograms[instrument.Name] = data.DataPoints[0]
			}
		}
	}
	require.Zero(t, values["balancehistory.builder.effects.processed"])
	require.Zero(t, values["balancehistory.builder.postings.processed"])
	require.Equal(t, int64(1), values["balancehistory.builder.publications"])
	require.Equal(t, uint64(1), histograms["balancehistory.builder.batch.duration"].Count)
	require.Equal(t, uint64(1), histograms["balancehistory.builder.batch.proposals"].Count)
	require.Equal(t, int64(2), histograms["balancehistory.builder.batch.proposals"].Sum)
	require.Equal(t, uint64(1), histograms["balancehistory.builder.publish_lag"].Count)
	require.Equal(t, int64(1500), histograms["balancehistory.builder.publish_lag"].Sum)
}

func TestBuilderProcessingMetricsRecordsOneResetAndRebuildPerCall(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := newBuilderProcessingMetrics(
		provider.Meter("balance-history-builder-rebuild-metrics-test"),
	)
	require.NoError(t, err)

	metrics.recordRebuild()

	var resources metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resources))
	values := map[string]int64{}
	for _, scope := range resources.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			data, ok := instrument.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			require.Len(t, data.DataPoints, 1)
			require.Empty(t, data.DataPoints[0].Attributes.ToSlice())
			values[instrument.Name] = data.DataPoints[0].Value
		}
	}

	require.Equal(t, int64(1), values["balancehistory.builder.resets"])
	require.Equal(t, int64(1), values["balancehistory.builder.rebuilds"])
}

func TestBuilderRoutesStoreSourceMissingToFailClosedRepair(t *testing.T) {
	t.Parallel()

	history := newBuilderTestHistoryStore(t)
	builder := newBuilderForTest(t, nil, history, nil)

	require.NoError(t, builder.handleBuildError(&balancehistorystore.ErrSourceMissing{
		Detail: "cold compaction input is absent",
	}))
	require.True(t, builder.sourceMissing.Load())
	require.True(t, builder.rebuildFromGenesis.Load())
	_, err := history.OpenView(0)
	var missing *balancehistorystore.ErrSourceMissing
	require.ErrorAs(t, err, &missing)
	var quarantined *balancehistorystore.ErrQuarantined
	require.False(t, errors.As(err, &quarantined))
}
