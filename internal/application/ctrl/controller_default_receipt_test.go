package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/receipt"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const testReceiptKey = "test-receipt-signing-key-32bytes!"

func newReceiptTestStore(t *testing.T) *dal.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// seedCreatedTransaction writes a ledger, the transaction-state attribute, and
// the transaction's CreatedTransaction creation log (carrying chapterID), so
// GetTransactionFrom can assemble the transaction and ComputeTransactionReceipt
// can resolve a receipt by reading only this store.
func seedCreatedTransaction(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string, txID, logSeq, chapterID uint64, tx *commonpb.Transaction) {
	t.Helper()

	batch := store.OpenWriteSession()

	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{Name: ledger}))

	txKey := domain.TransactionKey{LedgerName: ledger, ID: txID}
	_, err := attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{CreatedByLog: logSeq})
	require.NoError(t, err)

	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: logSeq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: tx,
									ChapterId:   chapterID,
								},
							},
						},
					},
				},
			},
		},
	}}))

	require.NoError(t, batch.Commit())
}

// seedRevertedTransaction is like seedCreatedTransaction but writes a
// RevertedTransaction creation log (as produced by reverting a transaction),
// which carries no receipt.
func seedRevertedTransaction(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string, txID, logSeq uint64, tx *commonpb.Transaction) {
	t.Helper()

	batch := store.OpenWriteSession()

	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{Name: ledger}))

	txKey := domain.TransactionKey{LedgerName: ledger, ID: txID}
	_, err := attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{CreatedByLog: logSeq})
	require.NoError(t, err)

	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: logSeq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
								RevertedTransaction: &commonpb.RevertedTransaction{
									RevertTransaction: tx,
								},
							},
						},
					},
				},
			},
		},
	}}))

	require.NoError(t, batch.Commit())
}

func newReceiptTestController(t *testing.T, store *dal.Store, attrs *attributes.Attributes, signer *receipt.Signer) *DefaultController {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	return NewDefaultController(nil, store, logger, attrs, nil, nil, signer, meter)
}

// GetTransaction on the local path must return a NON-EMPTY receipt when a signer
// is configured. This is the EN-1510 acceptance criterion: the receipt is now
// produced by the shared controller layer, so both gRPC and HTTP (which route
// through DefaultController) get it — not only the gRPC adapter.
func TestDefaultController_GetTransaction_LocalPath_SignsReceipt(t *testing.T) {
	t.Parallel()

	const (
		ledger    = "L"
		txID      = uint64(1)
		logSeq    = uint64(1)
		chapterID = uint64(7)
	)

	attrs := attributes.New()
	tx := &commonpb.Transaction{
		Id:        txID,
		Postings:  []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD"}},
		Timestamp: &commonpb.Timestamp{Data: 1700000000},
	}

	store := newReceiptTestStore(t)
	seedCreatedTransaction(t, store, attrs, ledger, txID, logSeq, chapterID, tx)

	signer := receipt.NewSigner([]byte(testReceiptKey))
	ctrl := newReceiptTestController(t, store, attrs, signer)

	gotTx, receiptToken, err := ctrl.GetTransaction(context.Background(), ledger, txID)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.Equal(t, txID, gotTx.GetId())

	require.NotNil(t, receiptToken, "a signing node must return a non-nil receipt on the local path")
	require.NotEmpty(t, *receiptToken, "a created transaction must have a non-empty receipt")

	claims, err := signer.Verify(*receiptToken)
	require.NoError(t, err)
	require.Equal(t, chapterID, claims.ChapterID)
}

// With no signer configured, GetTransaction returns a nil receipt (the historical
// behaviour of a non-signing node) while still returning the transaction.
func TestDefaultController_GetTransaction_NoSigner_NilReceipt(t *testing.T) {
	t.Parallel()

	const (
		ledger    = "L"
		txID      = uint64(1)
		logSeq    = uint64(1)
		chapterID = uint64(7)
	)

	attrs := attributes.New()
	tx := &commonpb.Transaction{Id: txID, Timestamp: &commonpb.Timestamp{Data: 1700000000}}

	store := newReceiptTestStore(t)
	seedCreatedTransaction(t, store, attrs, ledger, txID, logSeq, chapterID, tx)

	ctrl := newReceiptTestController(t, store, attrs, nil)

	gotTx, receiptToken, err := ctrl.GetTransaction(context.Background(), ledger, txID)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.Nil(t, receiptToken, "a node without a signer produces no receipt")
}

// A reversal transaction's creation log is a RevertedTransaction log, which has
// no receipt. GetTransaction must still succeed, returning a non-nil but empty
// receipt token (the signer ran but there was nothing to sign).
func TestDefaultController_GetTransaction_Reversal_EmptyReceipt(t *testing.T) {
	t.Parallel()

	const (
		ledger = "l"
		txID   = uint64(2)
		logSeq = uint64(5)
	)

	attrs := attributes.New()
	tx := &commonpb.Transaction{Id: txID, Timestamp: &commonpb.Timestamp{Data: 1700000000}}

	store := newReceiptTestStore(t)
	seedRevertedTransaction(t, store, attrs, ledger, txID, logSeq, tx)

	signer := receipt.NewSigner([]byte(testReceiptKey))
	ctrl := newReceiptTestController(t, store, attrs, signer)

	gotTx, receiptToken, err := ctrl.GetTransaction(context.Background(), ledger, txID)
	require.NoError(t, err)
	require.NotNil(t, gotTx)
	require.NotNil(t, receiptToken)
	require.Empty(t, *receiptToken, "reversal transactions have no receipt")
}

// ComputeTransactionReceipt must resolve the chapter from the supplied reader,
// not from the controller's live store. This is what keeps a checkpoint read
// self-consistent when the gRPC adapter passes the checkpoint store as reader.
func TestComputeTransactionReceipt_UsesProvidedReaderNotLiveStore(t *testing.T) {
	t.Parallel()

	const (
		ledger              = "L"
		txID                = uint64(1)
		logSeq              = uint64(1)
		checkpointChapterID = uint64(7)
		liveStoreChapterID  = uint64(99)
	)

	attrs := attributes.New()
	tx := &commonpb.Transaction{
		Postings:  []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD"}},
		Timestamp: &commonpb.Timestamp{Data: 1700000000},
	}

	checkpointStore := newReceiptTestStore(t)
	seedCreatedTransaction(t, checkpointStore, attrs, ledger, txID, logSeq, checkpointChapterID, tx)

	// The controller's live store holds the same transaction under a different
	// chapter; the receipt must reflect the reader (checkpoint) chapter.
	liveStore := newReceiptTestStore(t)
	seedCreatedTransaction(t, liveStore, attrs, ledger, txID, logSeq, liveStoreChapterID, tx)

	signer := receipt.NewSigner([]byte(testReceiptKey))
	ctrl := newReceiptTestController(t, liveStore, attrs, signer)

	reader, err := checkpointStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	token, err := ctrl.ComputeTransactionReceipt(context.Background(), reader, ledger, txID, tx)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := signer.Verify(token)
	require.NoError(t, err)
	require.Equal(t, checkpointChapterID, claims.ChapterID,
		"receipt must use the chapter from the reader (checkpoint), not the live store")
}

// A genuine reader/lookup error (here: the ledger is absent from the reader)
// must propagate rather than being masked into a receiptless success.
func TestComputeTransactionReceipt_PropagatesReaderError(t *testing.T) {
	t.Parallel()

	attrs := attributes.New()
	emptyReader := newReceiptTestStore(t)

	signer := receipt.NewSigner([]byte(testReceiptKey))
	ctrl := newReceiptTestController(t, newReceiptTestStore(t), attrs, signer)

	reader, err := emptyReader.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	tx := &commonpb.Transaction{Timestamp: &commonpb.Timestamp{Data: 1700000000}}

	_, err = ctrl.ComputeTransactionReceipt(context.Background(), reader, "missing", 1, tx)
	require.Error(t, err)
}
