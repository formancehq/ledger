package grpc

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

func newReceiptTestStore(t *testing.T) *dal.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// seedTransaction writes a ledger, a transaction-state attribute, and the
// transaction's creation log (carrying chapterID) into store, so that
// computeTransactionReceipt can resolve a receipt by reading only this store.
func seedTransaction(t *testing.T, store *dal.Store, attrs *attributes.Attributes, ledger string, txID, logSeq, chapterID uint64, tx *commonpb.Transaction) {
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

// A checkpoint read must compute its receipt from the checkpoint store, not the
// live store. Here the live store records a DIFFERENT chapter for the same
// transaction; the receipt must reflect the reader (checkpoint) chapter.
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
		Timestamp: 1700000000,
	}

	checkpointStore := newReceiptTestStore(t)
	seedTransaction(t, checkpointStore, attrs, ledger, txID, logSeq, checkpointChapterID, tx)

	// Live store holds the same transaction but under a different chapter.
	liveStore := newReceiptTestStore(t)
	seedTransaction(t, liveStore, attrs, ledger, txID, logSeq, liveStoreChapterID, tx)

	signer := receipt.NewSigner([]byte("test-receipt-signing-key-32bytes!"))
	impl := &BucketServiceServerImpl{
		attrs:         attrs,
		receiptSigner: signer,
		store:         liveStore,
	}

	token, err := impl.computeTransactionReceipt(context.Background(), checkpointStore, ledger, txID, tx)
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

	impl := &BucketServiceServerImpl{
		attrs:         attrs,
		receiptSigner: receipt.NewSigner([]byte("test-receipt-signing-key-32bytes!")),
		store:         newReceiptTestStore(t),
	}

	tx := &commonpb.Transaction{Timestamp: 1700000000}

	_, err := impl.computeTransactionReceipt(context.Background(), emptyReader, "missing", 1, tx)
	require.Error(t, err)
}

// seedRevertedTransaction is like seedTransaction but writes a RevertedTransaction
// creation log (as produced by reverting a transaction), which carries no receipt.
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

// A reversal transaction's creation log is a RevertedTransaction log, which has
// no receipt — matching the Apply path, which only issues receipts for created
// transactions. computeTransactionReceipt must return an empty receipt without
// error, so GetTransaction on a reversal still succeeds.
func TestComputeTransactionReceipt_RevertTransactionHasNoReceipt(t *testing.T) {
	t.Parallel()

	const (
		ledger = "l"
		txID   = uint64(2)
		logSeq = uint64(5)
	)

	attrs := attributes.New()
	store := newReceiptTestStore(t)
	tx := &commonpb.Transaction{Id: txID, Timestamp: 1700000000}
	seedRevertedTransaction(t, store, attrs, ledger, txID, logSeq, tx)

	impl := &BucketServiceServerImpl{
		attrs:         attrs,
		receiptSigner: receipt.NewSigner([]byte("test-receipt-signing-key-32bytes!")),
		store:         store,
	}

	token, err := impl.computeTransactionReceipt(context.Background(), store, ledger, txID, tx)
	require.NoError(t, err, "a reversal transaction must not fail receipt computation")
	require.Empty(t, token, "reversal transactions have no receipt")
}
