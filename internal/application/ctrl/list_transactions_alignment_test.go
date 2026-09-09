package ctrl

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A complement is the sharp cross-store consistency probe: if the primary
// snapshot sees a transaction while the independently taken index snapshot
// does not see its timestamp row, NOT(timestamp) manufactures a result that
// never existed at one serial point. Exercise the real controller path with
// the two stores deliberately at different horizons so a separate lag sample
// cannot race the read it purports to validate.
func TestListTransactions_AlignsComplementWithPrimaryHorizon(t *testing.T) {
	t.Parallel()

	const (
		ledger = "alignment"
		txID   = uint64(1)
		logSeq = uint64(1)
	)

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store := newCtrlTestStore(t)
	attrs := attributes.New()

	seedCreatedTransaction(t, store, attrs, ledger, txID, logSeq, &commonpb.Transaction{Id: txID})

	timestampID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	mainBatch := store.OpenWriteSession()
	_, err := attrs.Index.Set(mainBatch, indexes.KeyFor(ledger, timestampID).Bytes(), &commonpb.Index{
		Id:                     timestampID,
		Ledger:                 ledger,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, state.SetAppliedIndex(mainBatch, logSeq))
	require.NoError(t, mainBatch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })
	readyBatch := rs.NewBatch()
	require.NoError(t, rs.WriteIndexVersionState(readyBatch, ledger, indexes.Canonical(timestampID), readstore.IndexVersionState{
		CurrentVersion: 1,
		HighWater:      1,
	}))
	require.NoError(t, readyBatch.Commit())

	ctrl := NewDefaultController(nil, store, logger, attrs, rs, nil, meter)
	notTimestamp := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{
		Filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
			BuiltinUint: &commonpb.BuiltinUintCondition{
				Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
				Cond:  &commonpb.UintCondition{Min: new(uint64), Max: new(uint64)},
			},
		}},
	}}}
	*notTimestamp.GetNot().GetFilter().GetBuiltinUint().GetCond().Max = math.MaxUint64

	// The primary handle contains logSeq, but the projection cursor is still
	// zero and the timestamp row is absent. The same request must wait rather
	// than compile a complement over those torn views.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	unexpected, err := ctrl.ListTransactions(ctx, ledger, 100, 0, notTimestamp, false)
	if unexpected != nil {
		require.NoError(t, unexpected.Close())
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Fold the missing row and advance progress in one atomic batch, matching
	// the index builder's publication order. The complement is now decidable
	// at the primary horizon and must be empty.
	indexBatch := rs.NewBatch()
	wb := readstore.NewWriteBatch()
	wb.Init(indexBatch)
	require.NoError(t, wb.WriteTransactionTimestampIndex(dal.NewKeyBuilder(), ledger, 42, txID))
	require.NoError(t, rs.WriteProgress(indexBatch, logSeq))
	require.NoError(t, rs.WriteRaftProgress(indexBatch, logSeq))
	require.NoError(t, indexBatch.Commit())
	rs.NotifyProgress()

	txs, err := ctrl.ListTransactions(t.Context(), ledger, 100, 0, notTimestamp, false)
	require.NoError(t, err)

	got, err := cursor.Collect(txs)
	require.NoError(t, err)
	require.Empty(t, got)
}

// seedCreatedTransaction writes the minimum authoritative state required by
// transaction reads. Keep this fixture independent from optional response
// features so read-alignment tests survive unrelated API removals.
func seedCreatedTransaction(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	ledger string,
	txID, logSeq uint64,
	tx *commonpb.Transaction,
) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))

	txKey := domain.TransactionKey{LedgerName: ledger, ID: txID}
	_, err := attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{CreatedByLog: logSeq})
	require.NoError(t, err)

	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: logSeq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: tx}},
			}},
		}}},
	}}))
	require.NoError(t, batch.Commit())
}
