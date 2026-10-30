package admission

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestCheckMirrorPromotions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		cursor, head uint64
		syncError    bool
		blocked      bool
	}{
		{name: "never observed source", cursor: 0, head: 0, blocked: true},
		{name: "lagging", cursor: 3, head: 5, blocked: true},
		{name: "error at head", cursor: 5, head: 5, syncError: true, blocked: true},
		{name: "caught up", cursor: 5, head: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := createTestStore(t)
			a, attrs := createTestAdmission(t, store)
			batch := store.OpenWriteSession()
			require.NoError(t, state.SaveLedger(batch, testLedgerName,
				&commonpb.LedgerInfo{Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}))
			_, err := attrs.Boundary.Set(batch, []byte(testLedgerName),
				&raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: tc.cursor})
			require.NoError(t, err)
			require.NoError(t, state.SetMirrorSourceHead(batch, testLedgerName, tc.head))
			if tc.syncError {
				require.NoError(t, state.SetMirrorStatus(batch, testLedgerName,
					&commonpb.MirrorSyncError{Message: "source unavailable"}))
			}
			require.NoError(t, batch.Commit())
			orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: testLedgerName,
					Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
						PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
					},
				},
			}}}
			err = a.checkMirrorPromotions(t.Context(), orders, "")
			if !tc.blocked {
				require.NoError(t, err)

				return
			}
			var notCaughtUp *ErrMirrorNotCaughtUp
			require.ErrorAs(t, err, &notCaughtUp)
			require.Equal(t, tc.cursor, notCaughtUp.Cursor)
			require.Equal(t, tc.head, notCaughtUp.Head)
			require.Equal(t, tc.syncError, notCaughtUp.SyncError)
			_, _, _, err = a.prepareOrders(t.Context(), &admissionPlan{
				buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
					return orders, newBulkOverlay(), nil
				},
			})
			require.ErrorAs(t, err, &notCaughtUp, "admission must reject before proposing")
		})
	}
}

func TestCheckMirrorPromotionsRejectsUnreadyLedgerState(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}}},
	}}}
	var describable domain.Describable
	require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, ""), &describable)
	require.Equal(t, domain.ErrReasonLedgerNotInMirrorMode, describable.Reason())
}

func TestCheckMirrorPromotionsRejectsAbsentAndDefersDeletedLedgerToFSM(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, testLedgerName, &commonpb.LedgerInfo{
		Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
		DeletedAt: &commonpb.Timestamp{Data: 10},
	}))
	require.NoError(t, batch.Commit())
	for _, ledger := range []string{testLedgerName, "never-created"} {
		orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}}},
		}}}
		if ledger == testLedgerName {
			require.NoError(t, a.checkMirrorPromotions(t.Context(), orders, ""),
				"the FSM distinguishes deleted ledger rejections from keyed replays")
		} else {
			var notFound *domain.ErrLedgerNotFound
			require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, ""), &notFound,
				"an absent ledger must be rejected before a concurrent mirror creation")
		}
	}
}

func TestCheckMirrorPromotionsPreservesCommittedKeyReplay(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, testLedgerName,
		&commonpb.LedgerInfo{Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL}))
	require.NoError(t, state.SaveIdempotencyKey(batch, "committed-promotion", &commonpb.IdempotencyKeyValue{}))
	require.NoError(t, batch.Commit())
	orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}}},
	}}}
	require.NoError(t, a.checkMirrorPromotions(t.Context(), orders, "committed-promotion"),
		"an existing key must reach the FSM replay and conflict check")
	_, _, _, err := a.prepareOrders(t.Context(), &admissionPlan{
		key: "committed-promotion",
		buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
			return orders, newBulkOverlay(), nil
		},
	})
	require.NoError(t, err, "the committed key must pass the actual admission preparation path")
	var describable domain.Describable
	require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, "fresh-promotion"), &describable)
	require.Equal(t, domain.ErrReasonLedgerNotInMirrorMode, describable.Reason())
}

func TestCheckMirrorPromotionsPreservesReplayAfterLedgerDeletion(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, testLedgerName, &commonpb.LedgerInfo{
		Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
		DeletedAt: &commonpb.Timestamp{Data: 10},
	}))
	require.NoError(t, state.SaveIdempotencyKey(batch, "promote-and-delete", &commonpb.IdempotencyKeyValue{}))
	require.NoError(t, batch.Commit())
	orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}}},
	}}}
	_, _, _, err := a.prepareOrders(t.Context(), &admissionPlan{
		key: "promote-and-delete",
		buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
			return orders, newBulkOverlay(), nil
		},
	})
	require.NoError(t, err, "a committed keyed promotion must reach FSM replay after deletion")
	orders = append([]*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}}},
	}}}, orders...)
	var describable domain.Describable
	require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, "promote-and-delete"), &describable,
		"a retained key cannot bypass the create-then-promote safety gate")
	require.Equal(t, domain.ErrReasonLedgerNotFound, describable.Reason())
}

func TestCheckMirrorPromotionsDoesNotBypassUnreadyMirrorWithStoredKey(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, testLedgerName,
		&commonpb.LedgerInfo{Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}))
	require.NoError(t, state.SaveIdempotencyKey(batch, "expired-promotion", &commonpb.IdempotencyKeyValue{
		CreatedAt: 1, ExpiresAt: 2,
	}))
	require.NoError(t, state.SaveIdempotencyKey(batch, "live-promotion", &commonpb.IdempotencyKeyValue{
		CreatedAt: 1,
	}))
	require.NoError(t, batch.Commit())
	orders := []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}}},
	}}}
	for _, key := range []string{"expired-promotion", "live-promotion"} {
		t.Run(key, func(t *testing.T) {
			var notCaughtUp *ErrMirrorNotCaughtUp
			require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, key), &notCaughtUp)
			_, _, _, err := a.prepareOrders(t.Context(), &admissionPlan{
				key: key,
				buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
					return orders, newBulkOverlay(), nil
				},
			})
			require.ErrorAs(t, err, &notCaughtUp, "stored keys cannot permit a fresh unsafe promotion")
		})
	}
}

func TestCheckMirrorPromotionsReplaysFrozenBatchFailureOnUnreadyMirror(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  testLedgerName,
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
		}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "missing-ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
		}}},
	}
	failure := &domain.ErrLedgerNotFound{Name: "missing-ledger"}
	now := uint64(time.Now().UnixMicro())
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, testLedgerName,
		&commonpb.LedgerInfo{Name: testLedgerName, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}))
	for key, value := range map[string]*commonpb.IdempotencyKeyValue{
		"frozen-failure": {
			Hash: processing.HashOrders(orders), ExpiresAt: now + uint64(time.Hour/time.Microsecond),
			Failure: &commonpb.IdempotencyFailure{Details: domain.TypedFailureOf(failure)},
		},
		"expired-failure": {
			Hash: processing.HashOrders(orders), ExpiresAt: now - 1,
			Failure: &commonpb.IdempotencyFailure{Details: domain.TypedFailureOf(failure)},
		},
		"different-batch": {
			Hash: processing.HashOrders(orders[:1]), ExpiresAt: now + uint64(time.Hour/time.Microsecond),
			Failure: &commonpb.IdempotencyFailure{Details: domain.TypedFailureOf(failure)},
		},
	} {
		require.NoError(t, state.SaveIdempotencyKey(batch, key, value))
	}
	require.NoError(t, batch.Commit())

	for _, tc := range []struct {
		key        string
		wantReplay bool
	}{
		{key: "frozen-failure", wantReplay: true},
		{key: "expired-failure"},
		{key: "different-batch"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			_, _, _, err := a.prepareOrders(t.Context(), &admissionPlan{
				key: tc.key,
				buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
					return orders, newBulkOverlay(), nil
				},
			})
			if tc.wantReplay {
				var replay *domain.ReplayedFailure
				require.ErrorAs(t, err, &replay)
				require.Equal(t, domain.ErrReasonLedgerNotFound, replay.Reason())
				require.Equal(t, "ledger not found: name=\"missing-ledger\"", replay.Error())
			} else {
				var notCaughtUp *ErrMirrorNotCaughtUp
				require.ErrorAs(t, err, &notCaughtUp)
			}
		})
	}
	batch = store.OpenWriteSession()
	require.NoError(t, state.StoreLastAppliedTimestamp(batch, now+uint64(2*time.Hour/time.Microsecond)))
	require.NoError(t, batch.Commit())
	var notCaughtUp *ErrMirrorNotCaughtUp
	require.ErrorAs(t, a.checkMirrorPromotions(t.Context(), orders, "frozen-failure"), &notCaughtUp,
		"an expired failure must not authorize a fresh promotion when the HLC is ahead of wall time")
}

func TestPrepareOrdersRejectsCreateThenPromoteMirror(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "new-mirror", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}}}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "new-mirror", Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
		}}},
	}
	_, _, _, err := a.prepareOrders(t.Context(), &admissionPlan{
		buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
			return orders, newBulkOverlay(), nil
		},
	})
	var describable domain.Describable
	require.ErrorAs(t, err, &describable)
	require.Equal(t, domain.ErrReasonLedgerNotFound, describable.Reason())
}
