package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func testLogger() logging.Logger {
	return logging.FromContext(logging.TestingContext())
}

func newRebuildTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func coldLogKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(seq).
		Build()
}

func coldAuditKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(seq).
		Build()
}

// createLedgerLog builds a log whose replay writes a ledger row to the global
// zone, so the test can observe whether the rebuild batch was committed.
func createLedgerLog(seq uint64, name string, id uint32) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{Name: name, Id: id},
			},
		},
	}
}

func applyLedgerLog(seq uint64, ledger string, payload *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:   seq,
						Data: payload,
					},
				},
			},
		},
	}
}

func addAccountTypePayload(name, pattern string, persistence commonpb.AccountTypePersistence) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     pattern,
					Persistence: persistence,
				},
			},
		},
	}
}

func createdTransactionPayload(id uint64, postings ...*commonpb.Posting) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Id:       id,
					Postings: postings,
				},
			},
		},
	}
}

func rebuildTestPosting(source, destination, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

// auditItem serializes order into an AuditItem the way the FSM persists it,
// so applyAuditOrderEffects can decode the order back out.
func auditItem(t *testing.T, logSeq uint64, order *raftcmdpb.Order) *auditpb.AuditItem {
	t.Helper()

	raw, err := order.MarshalVT()
	require.NoError(t, err)

	return &auditpb.AuditItem{LogSequence: logSeq, SerializedOrder: raw}
}

func fillGapOrder(ledger string, v2LogID uint64, skippedIDs ...uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Entry: &raftcmdpb.MirrorLogEntry{
							V2LogId: v2LogID,
							Data:    &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{SkippedTransactionIds: skippedIDs}},
						},
					},
				},
			},
		},
	}
}

func auditSuccess(seq, minLogSeq, maxLogSeq uint64) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence: seq,
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: minLogSeq,
				MaxLogSequence: maxLogSeq,
			},
		},
	}
}

func TestRebuildDelta_CleanEOFSucceeds(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	for seq := uint64(1); seq <= 3; seq++ {
		require.NoError(t, batch.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, batch.Commit())

	// A clean stream must terminate via io.EOF and report success.
	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	// Derived state was committed.
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	info, err := query.GetLedgerByName(context.Background(), handle, "ledger")
	require.NoError(t, err)
	require.NotNil(t, info, "ledger should have been rebuilt on a clean stream")
}

func TestRebuildDelta_TruncatedStreamReturnsErrorAndDoesNotCommit(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	// Valid log at seq 1 (creates "before" ledger), then a corrupt record at
	// seq 2 whose bytes fail to unmarshal — simulating a truncated/corrupted
	// log stream during a restore.
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "before-corruption", 1)))
	require.NoError(t, batch.SetBytes(coldLogKey(2), []byte{0xff, 0xff, 0xff, 0xff}))
	require.NoError(t, batch.Commit())

	err := RebuildDelta(context.Background(), testLogger(), store, 0, 0)

	// The non-EOF cursor error must surface, not be swallowed as success.
	require.Error(t, err, "RebuildDelta must not report success on a truncated stream")

	// And the partial batch (the seq-1 ledger processed before the corrupt
	// record) must have been cancelled, not committed.
	handle, err2 := store.NewDirectReadHandle()
	require.NoError(t, err2)
	defer func() { _ = handle.Close() }()

	_, err2 = query.GetLedgerByName(context.Background(), handle, "before-corruption")
	require.ErrorIs(t, err2, domain.ErrNotFound,
		"partial rebuild state must not be committed when the stream errors")
}

func TestRebuildDelta_ReplaysEphemeralPurgeAtProposalBoundary(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger",
		addAccountTypePayload("orders", "orders:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(3), applyLedgerLog(3, "ledger",
		createdTransactionPayload(1, rebuildTestPosting("world", "orders:1", "USD", 5)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(4), applyLedgerLog(4, "ledger",
		createdTransactionPayload(2, rebuildTestPosting("orders:1", "world", "USD", 5)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(5), applyLedgerLog(5, "ledger",
		createdTransactionPayload(3, rebuildTestPosting("world", "orders:1", "USD", 3)),
	)))
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditSuccess(2, 2, 2)))
	require.NoError(t, batch.SetProto(coldAuditKey(3), auditSuccess(3, 3, 5)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()
	pair, err := attrs.Volume.Get(handle, domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "orders:1"},
		Asset:      "USD",
	}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair)
	require.Equal(t, "8", pair.GetInput().ToBigInt().String())
	require.Equal(t, "5", pair.GetOutput().ToBigInt().String())

	// Boundaries are rebuilt into the attribute zone with the id fields. Log
	// ids here equal the log sequence (test fixture), so NextLogId is
	// max(seq)+1 over the four apply logs (seqs 2-5). The per-ledger usage
	// counters live in the usagestore peer secondary store, not here.
	boundary, err := attrs.Boundary.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, boundary, "boundaries must be reconstructed into the attribute zone")
	require.Equal(t, uint64(4), boundary.GetNextTransactionId(), "3 transactions created")
	require.Equal(t, uint64(6), boundary.GetNextLogId())
}

func TestRebuildDelta_ReconstructsTransactionReference(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))

	tx := createdTransactionPayload(1, rebuildTestPosting("world", "alice", "USD", 100))
	tx.GetCreatedTransaction().GetTransaction().Reference = "ref-1"
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger", tx)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	ref, err := attrs.References.Get(handle, domain.TransactionReferenceKey{LedgerName: "ledger", Reference: "ref-1"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, ref, "reference index must be reconstructed")
	require.Equal(t, uint64(1), ref.GetTransactionId())
}

func TestRebuildDelta_AdvancesBoundariesForMirrorFillGap(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_FillGap{FillGap: &commonpb.FilledGapLog{OriginalId: 7}},
	})))
	require.NoError(t, batch.SetProto(coldAuditItemKey(1, 0), auditItem(t, 2, fillGapOrder("ledger", 7, 5, 9))))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	boundary, err := attrs.Boundary.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, boundary)
	require.Equal(t, uint64(10), boundary.GetNextTransactionId(),
		"the fill-gap's highest skipped id (9) must advance NextTransactionId to 10; the ids live on the order in AuditItem, not on FilledGapLog")
	require.Equal(t, uint64(3), boundary.GetNextLogId())
}

func TestRebuildDelta_ReplaysLedgerMetadata(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	strValue := func(s string) *commonpb.MetadataValue {
		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: s}}
	}

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SavedLedgerMetadata{
				SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{
					Ledger:   "ledger",
					Metadata: map[string]*commonpb.MetadataValue{"env": strValue("prod"), "tier": strValue("gold")},
				},
			},
		},
	}))
	require.NoError(t, batch.SetProto(coldLogKey(3), &commonpb.Log{
		Sequence: 3,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletedLedgerMetadata{
				DeletedLedgerMetadata: &commonpb.DeletedLedgerMetadataLog{Ledger: "ledger", Key: "tier"},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	env, err := attrs.LedgerMetadata.Get(handle, domain.LedgerMetadataKey{LedgerName: "ledger", Key: "env"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, env, "ledger metadata saved after the checkpoint must be rebuilt")
	require.Equal(t, "prod", env.GetStringValue())

	tier, err := attrs.LedgerMetadata.Get(handle, domain.LedgerMetadataKey{LedgerName: "ledger", Key: "tier"}.Bytes())
	require.NoError(t, err)
	require.Nil(t, tier, "ledger metadata deleted after the checkpoint must not resurrect")
}

func TestRebuildDelta_ReplaysDeleteLedger(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger",
		createdTransactionPayload(1, rebuildTestPosting("world", "alice", "USD", 10)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(3), &commonpb.Log{
		Sequence: 3,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{
				DeleteLedger: &commonpb.DeletedLedgerLog{Name: "ledger", DeletedAt: &commonpb.Timestamp{Data: 999}},
			},
		},
	}))
	// Pre-delete audit activity must not resurrect a boundary row for the
	// deleted ledger.
	require.NoError(t, batch.SetProto(coldAuditItemKey(1, 0), auditItem(t, 2, fillGapOrder("ledger", 7, 42))))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	info, err := attrs.Ledger.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, info, "the tombstoned LedgerInfo is kept for deleted-ledger responses")
	require.Equal(t, uint64(999), info.GetDeletedAt().GetData())

	boundary, err := attrs.Boundary.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.Nil(t, boundary, "the boundary row is dropped at delete time, matching the live apply")

	cleanupKey := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerNameFixed("ledger").
		Build()
	val, closer, err := store.Get(cleanupKey)
	require.NoError(t, err, "the pending-cleanup marker must be recorded so a covering purge executes the deferred data cleanup")
	require.Len(t, val, 8)
	require.NoError(t, closer.Close())
}

func TestRebuildDelta_ReplaysPromoteLedger(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name:         "ledger",
					Id:           1,
					Mode:         commonpb.LedgerMode_LEDGER_MODE_MIRROR,
					MirrorSource: &commonpb.MirrorSourceConfig{},
				},
			},
		},
	}))
	require.NoError(t, batch.SetProto(coldLogKey(2), &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_PromoteLedger{
				PromoteLedger: &commonpb.PromotedLedgerLog{Name: "ledger"},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	info, err := attrs.Ledger.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, info.GetMode(), "promotion must end mirror mode")
	require.Nil(t, info.GetMirrorSource(), "promotion must clear the mirror source")
}

func TestRebuildDelta_ReplaysDefaultEnforcementMode(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
			UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{
				EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			},
		},
	})))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()

	info, err := attrs.Ledger.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.GetDefaultEnforcementMode())
}

// TestRebuildDelta_ReconstructsFullLedgerInfoFromCreateLog: a post-checkpoint
// CreateLedger must restore the full LedgerInfo — Id, AccountTypes, and
// DefaultEnforcementMode — not just Name/Mode/schema.
func TestRebuildDelta_ReconstructsFullLedgerInfoFromCreateLog(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	createLog := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name: "ledger",
					Id:   42,
					AccountTypes: map[string]*commonpb.AccountType{
						"orders": {Name: "orders", Pattern: "orders:{id}"},
					},
					DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
				},
			},
		},
	}

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLog))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	info, err := query.GetLedgerByName(context.Background(), handle, "ledger")
	require.NoError(t, err)
	require.Equal(t, uint32(42), info.GetId())
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.GetDefaultEnforcementMode())
	require.Contains(t, info.GetAccountTypes(), "orders")
	require.Equal(t, "orders:{id}", info.GetAccountTypes()["orders"].GetPattern())

	// The FSM hot path / index builder / preload read LedgerInfo from the
	// SubAttrLedger attribute projection, not the Global zone — it must carry the
	// reconstructed info too.
	attrs := attributes.New()
	attrInfo, err := attrs.Ledger.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, attrInfo, "LedgerInfo must be written to the SubAttrLedger attribute, not only the Global zone")
	require.Equal(t, uint32(42), attrInfo.GetId())
	require.Contains(t, attrInfo.GetAccountTypes(), "orders")
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, attrInfo.GetDefaultEnforcementMode())
}

// TestRebuildDelta_PersistsPostCheckpointAccountTypeToLedgerInfo: an
// AddAccountType replayed after the checkpoint must fold onto LedgerInfo and
// persist to both the Global zone and the SubAttrLedger attribute, or the chart
// change is lost on restore.
func TestRebuildDelta_PersistsPostCheckpointAccountTypeToLedgerInfo(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger",
		addAccountTypePayload("orders", "orders:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL),
	)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Global zone.
	info, err := query.GetLedgerByName(context.Background(), handle, "ledger")
	require.NoError(t, err)
	require.Contains(t, info.GetAccountTypes(), "orders", "post-checkpoint account type must persist to the Global LedgerInfo")

	// SubAttrLedger attribute (what the FSM reads).
	attrs := attributes.New()
	attrInfo, err := attrs.Ledger.Get(handle, domain.LedgerKey{Name: "ledger"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, attrInfo)
	require.Contains(t, attrInfo.GetAccountTypes(), "orders", "post-checkpoint account type must persist to the SubAttrLedger attribute")
}

// TestRebuildDelta_SeedsInitialAccountTypesForEphemeralPurge: account types
// declared at ledger creation (on the CreateLedger log, not a later
// AddAccountType) must seed the replay's compiled-type maps, so the
// ephemeral-purge simulation classifies a matching account and purges it when it
// nets to zero within a proposal.
func TestRebuildDelta_SeedsInitialAccountTypesForEphemeralPurge(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	createLog := &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{
					Name: "ledger",
					Id:   1,
					AccountTypes: map[string]*commonpb.AccountType{
						"orders": {
							Name:        "orders",
							Pattern:     "orders:{id}",
							Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
						},
					},
				},
			},
		},
	}

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLog))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger",
		createdTransactionPayload(1, rebuildTestPosting("world", "orders:1", "USD", 5)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(3), applyLedgerLog(3, "ledger",
		createdTransactionPayload(2, rebuildTestPosting("orders:1", "world", "USD", 5)),
	)))
	// logs 2-3 form one proposal, so orders:1 nets to zero at the boundary.
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditSuccess(2, 2, 3)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()
	pair, err := attrs.Volume.Get(handle, domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "orders:1"},
		Asset:      "USD",
	}.Bytes())
	require.NoError(t, err)
	require.Nil(t, pair, "balanced ephemeral account should have been purged")
}

// newAttributeReplayWriter builds an isolated writer for regression tests of
// EN-1425: an in-batch create followed by a mutate must produce the merged
// state, not overwrite it with a fresh zero-value TransactionState.
func newAttributeReplayWriter(t *testing.T) (*attributeReplayWriter, *attributes.Attributes, *dal.Store) {
	t.Helper()

	store := newRebuildTestStore(t)
	attrs := attributes.New()

	readHandle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readHandle.Close() })

	writer := &attributeReplayWriter{
		store:           store,
		batch:           store.OpenWriteSession(),
		volume:          attrs.Volume,
		metadata:        attrs.Metadata,
		tx:              attrs.Transaction,
		ledger:          attrs.Ledger,
		references:      attrs.References,
		boundary:        attrs.Boundary,
		pendingVolumes:  make(map[string]*raftcmdpb.VolumePair),
		pendingTx:       make(map[string]*commonpb.TransactionState),
		ledgerInfos:     make(map[string]*commonpb.LedgerInfo),
		boundaries:      make(map[string]*raftcmdpb.LedgerBoundaries),
		reversions:      make(map[string]*bitset.Bitset),
		dirtyReversions: make(map[string]struct{}),
		readHandle:      readHandle,
	}
	t.Cleanup(func() { _ = writer.batch.Cancel() })

	return writer, attrs, store
}

func rebuildTestMetaMap(entries ...string) map[string]*commonpb.MetadataValue {
	m := make(map[string]*commonpb.MetadataValue, len(entries)/2)
	for i := 0; i < len(entries); i += 2 {
		m[entries[i]] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: entries[i+1]}}
	}

	return m
}

// TestAttributeReplayWriter_SetRevertedByPreservesCreatedByLog is the EN-1425
// revert scenario: CreateTransaction then SetRevertedBy in the same batch must
// not drop CreatedByLog/Timestamp/Metadata by re-reading committed state.
func TestAttributeReplayWriter_SetRevertedByPreservesCreatedByLog(t *testing.T) {
	t.Parallel()

	writer, attrs, store := newAttributeReplayWriter(t)
	key := domain.TransactionKey{LedgerName: "ledger", ID: 42}.Bytes()

	require.NoError(t, writer.CreateTransaction(key, 42,
		&commonpb.Timestamp{Data: 100},
		rebuildTestMetaMap("env", "prod"),
		[]*commonpb.Posting{rebuildTestPosting("world", "orders:1", "USD", 5)},
		0,
	))
	require.NoError(t, writer.SetRevertedBy(key, 99, &commonpb.Timestamp{Data: 150}))
	require.NoError(t, writer.batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	got, err := attrs.Transaction.Get(handle, key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(42), got.GetCreatedByLog(), "CreatedByLog must survive same-batch revert")
	require.Equal(t, uint64(99), got.GetRevertedByTransaction())
	require.Equal(t, uint64(150), got.GetRevertedAt().GetData(), "RevertedAt must survive same-batch revert")
	require.Equal(t, uint64(100), got.GetTimestamp().GetData(), "Timestamp must survive same-batch revert")
	require.Equal(t, "prod", got.GetMetadata()["env"].GetStringValue(), "Metadata must survive same-batch revert")
	require.Len(t, got.GetPostings(), 1, "Postings must survive same-batch revert")
}

// TestAttributeReplayWriter_SaveTxMetadataPreservesCreatedByLog is the EN-1425
// metadata-upsert scenario: CreateTransaction then SaveTxMetadata in the same
// batch must merge into the existing state, not clobber it.
func TestAttributeReplayWriter_SaveTxMetadataPreservesCreatedByLog(t *testing.T) {
	t.Parallel()

	writer, attrs, store := newAttributeReplayWriter(t)
	key := domain.TransactionKey{LedgerName: "ledger", ID: 43}.Bytes()

	require.NoError(t, writer.CreateTransaction(key, 43,
		&commonpb.Timestamp{Data: 200},
		rebuildTestMetaMap("env", "prod"),
		nil,
		0,
	))
	require.NoError(t, writer.SaveTxMetadata(key, rebuildTestMetaMap("region", "eu-west")))
	require.NoError(t, writer.batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	got, err := attrs.Transaction.Get(handle, key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(43), got.GetCreatedByLog())
	require.Equal(t, uint64(200), got.GetTimestamp().GetData())
	require.Equal(t, "prod", got.GetMetadata()["env"].GetStringValue(), "pre-existing metadata must survive upsert")
	require.Equal(t, "eu-west", got.GetMetadata()["region"].GetStringValue(), "new metadata must be merged in")
}

// TestAttributeReplayWriter_TwoMetadataUpsertsInSameBatchMerge exercises the
// second EN-1425 sub-scenario: two SaveTxMetadata calls within the same batch
// window must both persist, not clobber each other.
func TestAttributeReplayWriter_TwoMetadataUpsertsInSameBatchMerge(t *testing.T) {
	t.Parallel()

	writer, attrs, store := newAttributeReplayWriter(t)
	key := domain.TransactionKey{LedgerName: "ledger", ID: 44}.Bytes()

	require.NoError(t, writer.CreateTransaction(key, 44,
		&commonpb.Timestamp{Data: 300}, nil, nil, 0,
	))
	require.NoError(t, writer.SaveTxMetadata(key, rebuildTestMetaMap("status", "pending")))
	require.NoError(t, writer.SaveTxMetadata(key, rebuildTestMetaMap("owner", "alice")))
	require.NoError(t, writer.batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	got, err := attrs.Transaction.Get(handle, key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "pending", got.GetMetadata()["status"].GetStringValue())
	require.Equal(t, "alice", got.GetMetadata()["owner"].GetStringValue())
}

// TestAttributeReplayWriter_DeleteTxMetadataSeesPendingCreate is the EN-1425
// delete scenario: without the overlay the read returns nil and the delete is
// a silent no-op, leaving stale metadata after commit.
func TestAttributeReplayWriter_DeleteTxMetadataSeesPendingCreate(t *testing.T) {
	t.Parallel()

	writer, attrs, store := newAttributeReplayWriter(t)
	key := domain.TransactionKey{LedgerName: "ledger", ID: 45}.Bytes()

	require.NoError(t, writer.CreateTransaction(key, 45,
		&commonpb.Timestamp{Data: 400},
		rebuildTestMetaMap("env", "prod", "region", "eu-west"),
		nil,
		0,
	))
	require.NoError(t, writer.DeleteTxMetadata(key, "env"))
	require.NoError(t, writer.batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	got, err := attrs.Transaction.Get(handle, key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotContains(t, got.GetMetadata(), "env", "same-batch delete must actually remove the key")
	require.Equal(t, "eu-west", got.GetMetadata()["region"].GetStringValue())
}

// TestAttributeReplayWriter_SchemaOpForUnknownLedgerFailsLoudly: a schema op for
// a ledger that was neither in the checkpoint nor created during replay is an
// impossible/corrupt log stream, so it must surface loudly rather than silently
// drop the declaration (invariant 7).
func TestAttributeReplayWriter_SchemaOpForUnknownLedgerFailsLoudly(t *testing.T) {
	t.Parallel()

	writer, _, _ := newAttributeReplayWriter(t)
	writer.ledgerInfos = make(map[string]*commonpb.LedgerInfo)

	setErr := writer.SetMetadataFieldType("ghost", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k",
		commonpb.MetadataType_METADATA_TYPE_STRING)
	require.ErrorContains(t, setErr, "invariant")

	removeErr := writer.RemoveMetadataFieldType("ghost", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k")
	require.ErrorContains(t, removeErr, "invariant")
}

// TestAttributeReplayWriter_RemoveFieldTypeNoSchemaIsNoOp: removing a field from
// a known ledger that has no schema is a benign no-op (the field is already
// absent), not the impossible missing-ledger case.
func TestAttributeReplayWriter_RemoveFieldTypeNoSchemaIsNoOp(t *testing.T) {
	t.Parallel()

	writer, _, _ := newAttributeReplayWriter(t)
	writer.ledgerInfos = map[string]*commonpb.LedgerInfo{"ledger": {Name: "ledger"}}

	require.NoError(t, writer.RemoveMetadataFieldType("ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k"))
}

// TestAttributeReplayWriter_DeleteLedgerRemovesReversionRows: DeleteLedger
// replay must delete the ledger's persisted reversion words — the live path
// does so at apply time (not at the covering purge), so leaving them would
// resurrect a deleted ledger's bitset into Registry.Reversions on boot.
func TestAttributeReplayWriter_DeleteLedgerRemovesReversionRows(t *testing.T) {
	t.Parallel()

	writer, _, store := newAttributeReplayWriter(t)

	// Checkpoint-time reversion rows for the ledger about to be deleted.
	require.NoError(t, state.SaveReversionWord(writer.batch, "doomed", 0, 1<<3))
	require.NoError(t, state.SaveReversionWord(writer.batch, "doomed", 1, 1<<7))

	writer.ledgerInfos["doomed"] = &commonpb.LedgerInfo{Name: "doomed"}
	writer.reversions["doomed"] = &bitset.Bitset{}

	require.NoError(t, writer.deleteLedger("doomed", &commonpb.Timestamp{Data: 42}, 7))
	require.NoError(t, writer.batch.Commit())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	bs, err := query.ReadReversionBitset(handle, "doomed")
	require.NoError(t, err)
	require.Empty(t, bs.Words(), "deleted ledger's reversion rows must not survive the replay")
	require.Empty(t, writer.reversions["doomed"])
}

// keyedAuditSuccess / keyedAuditFailure build audit entries carrying an
// idempotency key and timestamp, as the rebuild reads them to reconstruct the
// SubIdempKeys projection.
func keyedAuditSuccess(seq uint64, key string, tsMicros, minLog, maxLog uint64) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence:    seq,
		Timestamp:   &commonpb.Timestamp{Data: tsMicros},
		Idempotency: &commonpb.Idempotency{Key: key},
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{MinLogSequence: minLog, MaxLogSequence: maxLog},
		},
	}
}

func keyedAuditFailure(seq uint64, key string, tsMicros uint64, reason commonpb.ErrorReason) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence:    seq,
		Timestamp:   &commonpb.Timestamp{Data: tsMicros},
		Idempotency: &commonpb.Idempotency{Key: key},
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{Reason: reason},
		},
	}
}

const (
	idemConflict     = commonpb.ErrorReason_ERROR_REASON_IDEMPOTENCY_KEY_CONFLICT
	idemFreshFailure = commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS
)

// A same-key/different-body reuse is audited as an IDEMPOTENCY_KEY_CONFLICT
// failure. The FSM only produces it against a live prior and never overwrites
// that prior, so the rebuild must skip it — keeping the original, not clobbering
// it with the conflict (which would make the original body conflict after
// restore).
func TestRebuildDelta_IdempotencyConflictKeepsDeltaOutcome(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	const key = "idem-key"

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldAuditKey(1), keyedAuditSuccess(1, key, 1_000_000, 1, 1)))
	require.NoError(t, batch.SetProto(coldAuditItemKey(1, 0), auditItem(t, 0, fillGapOrder("l", 1))))
	require.NoError(t, batch.SetProto(coldAuditKey(2), keyedAuditFailure(2, key, 2_000_000, idemConflict)))
	require.NoError(t, batch.SetProto(coldAuditItemKey(2, 0), auditItem(t, 0, fillGapOrder("l", 2))))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	v, err := state.LoadIdempotencyKey(handle, key)
	require.NoError(t, err)
	require.NotNil(t, v, "the committed keyed outcome must be rebuilt")
	require.Nil(t, v.GetFailure(), "the conflict must not overwrite the original")
	require.Equal(t, uint64(1), v.GetFirstLogSequence(), "the original success survives")
}

// Same, but the original outcome lives in the checkpoint SSTs (present in the
// store before the rebuild) rather than an earlier delta entry — the rebuild
// skips the delta conflict, so nothing overwrites it.
func TestRebuildDelta_IdempotencyConflictKeepsCheckpointOutcome(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	const key = "idem-key"

	batch := store.OpenWriteSession()
	// Original success frozen in the "checkpoint" (already in the store).
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
		FirstLogSequence: 1, LogCount: 1, CreatedAt: 1_000_000,
	}))
	// Only the later conflict is in the exported delta.
	require.NoError(t, batch.SetProto(coldAuditKey(1), keyedAuditFailure(1, key, 2_000_000, idemConflict)))
	require.NoError(t, batch.SetProto(coldAuditItemKey(1, 0), auditItem(t, 0, fillGapOrder("l", 2))))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	v, err := state.LoadIdempotencyKey(handle, key)
	require.NoError(t, err)
	require.NotNil(t, v)
	require.Nil(t, v.GetFailure(), "a delta conflict must not overwrite the checkpoint outcome")
	require.Equal(t, uint64(1), v.GetFirstLogSequence())
}

// A genuine (non-conflict) failure DOES overwrite an earlier outcome: it is a
// reuse the FSM's gate executed fresh because the prior had expired, so the new
// freeze is real. Unlike a conflict it carries a normal business reason and must
// materialize.
func TestRebuildDelta_IdempotencyFreshFailureOverwrites(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	const key = "idem-key"

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldAuditKey(1), keyedAuditSuccess(1, key, 1_000_000, 1, 1)))
	require.NoError(t, batch.SetProto(coldAuditItemKey(1, 0), auditItem(t, 0, fillGapOrder("l", 1))))
	require.NoError(t, batch.SetProto(coldAuditKey(2), keyedAuditFailure(2, key, 2_000_000, idemFreshFailure)))
	require.NoError(t, batch.SetProto(coldAuditItemKey(2, 0), auditItem(t, 0, fillGapOrder("l", 2))))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	v, err := state.LoadIdempotencyKey(handle, key)
	require.NoError(t, err)
	require.NotNil(t, v)
	require.NotNil(t, v.GetFailure(), "a fresh non-conflict failure overwrites the earlier outcome")
}
