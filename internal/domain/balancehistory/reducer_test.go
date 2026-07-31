package balancehistory_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestReducerDirectTransaction(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))

	effects, err := reducer.Reduce(
		position(2, 3, 11),
		transactionLog(11, "default", createdTransaction(posting("users:001", "merchant", "USD/2", "BLUE", 42), 100, 200)),
	)
	require.NoError(t, err)
	require.Equal(t, []balancehistory.Effect{
		{
			LedgerID: 7, AuditSequence: 2, OrderIndex: 3, LogSequence: 11,
			EffectiveAt: 100, InsertedAt: 200, Account: "users:001",
			AssetBase: "USD", AssetPrecision: 2, Color: "BLUE", Output: amount(42),
		},
		{
			LedgerID: 7, AuditSequence: 2, OrderIndex: 3, LogSequence: 11,
			EffectiveAt: 100, InsertedAt: 200, Account: "merchant",
			AssetBase: "USD", AssetPrecision: 2, Color: "BLUE", Input: amount(42),
		},
	}, effects)
}

func TestReducerRevertUsesResolvedPostingsWithoutSecondInversion(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))

	reversePosting := posting("merchant", "users:001", "USD", "", 42)
	effects, err := reducer.Reduce(
		position(2, 0, 11),
		transactionLog(11, "default", revertedTransaction(reversePosting, 123, 456)),
	)
	require.NoError(t, err)
	require.Len(t, effects, 2)
	require.Equal(t, "merchant", effects[0].Account)
	require.Equal(t, amount(42), effects[0].Output)
	require.Equal(t, balancehistory.Amount{}, effects[0].Input)
	require.Equal(t, "users:001", effects[1].Account)
	require.Equal(t, amount(42), effects[1].Input)
	require.Equal(t, balancehistory.Amount{}, effects[1].Output)
}

func TestReducerConsumesNumscriptAndMirrorResolvedLogsIdentically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *commonpb.LedgerLogPayload
	}{
		{
			name:    "numscript created transaction",
			payload: createdTransaction(posting("orders:resolved", "fees", "EUR/4", "OPS", 9), 300, 400),
		},
		{
			name:    "mirror reverted transaction",
			payload: revertedTransaction(posting("mirror:resolved", "world", "EUR/4", "OPS", 9), 300, 400),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reducer := balancehistory.NewReducer()
			require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))
			effects, err := reducer.Reduce(position(2, 0, 11), transactionLog(11, "default", test.payload))
			require.NoError(t, err)
			require.Len(t, effects, 2)
			require.Equal(t, uint32(7), effects[0].LedgerID)
			require.Equal(t, "EUR", effects[0].AssetBase)
			require.Equal(t, uint8(4), effects[0].AssetPrecision)
			require.Equal(t, "OPS", effects[0].Color)
			require.Equal(t, uint64(300), effects[0].EffectiveAt)
			require.Equal(t, uint64(400), effects[0].InsertedAt)
		})
	}
}

func TestReducerSeparatesDeleteAndRecreateIncarnations(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))
	first, err := reducer.Reduce(position(2, 0, 11), transactionLog(11, "default", createdTransaction(posting("world", "cash", "USD", "", 1), 10, 20)))
	require.NoError(t, err)
	require.NoError(t, reduceLifecycle(reducer, 3, 0, 12, deleteLedgerLog(12, "default")))
	require.NoError(t, reduceLifecycle(reducer, 4, 0, 13, createLedgerLog(13, "default", 99)))
	second, err := reducer.Reduce(position(5, 0, 14), transactionLog(14, "default", createdTransaction(posting("world", "cash", "USD", "", 2), 30, 40)))
	require.NoError(t, err)

	require.Equal(t, uint32(7), first[0].LedgerID)
	require.Equal(t, uint32(99), second[0].LedgerID)
}

func TestReducerNonMonetaryLogProducesNoEffectAndAdvancesOrder(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))

	effects, err := reducer.Reduce(position(2, 0, 11), transactionLog(11, "default", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{}},
	}))
	require.NoError(t, err)
	require.Empty(t, effects)

	_, err = reducer.Reduce(position(2, 0, 11), transactionLog(11, "default", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{}},
	}))
	require.ErrorIs(t, err, balancehistory.ErrOutOfOrder)
}

func TestReducerFailsClosedWithoutActiveIncarnation(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	_, err := reducer.Reduce(
		position(1, 0, 10),
		transactionLog(10, "missing", createdTransaction(posting("world", "cash", "USD", "", 1), 10, 20)),
	)
	require.ErrorIs(t, err, balancehistory.ErrMissingIncarnation)

	// An error must not advance the reducer, so the corrected source log at
	// the same position can still establish the missing incarnation.
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "missing", 5)))
}

func TestReducerRejectsReusedIncarnationID(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "first", 7)))
	require.NoError(t, reduceLifecycle(reducer, 2, 0, 11, deleteLedgerLog(11, "first")))
	err := reduceLifecycle(reducer, 3, 0, 12, createLedgerLog(12, "second", 7))
	require.ErrorIs(t, err, balancehistory.ErrInvalidLifecycle)
}

func TestReducerRejectsSequenceMismatch(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	_, err := reducer.Reduce(position(1, 0, 10), createLedgerLog(11, "default", 7))
	require.ErrorIs(t, err, balancehistory.ErrInvalidPosition)
}

func reduceLifecycle(reducer *balancehistory.Reducer, audit uint64, order uint32, logSequence uint64, log *commonpb.Log) error {
	effects, err := reducer.Reduce(position(audit, order, logSequence), log)
	if err != nil {
		return err
	}
	if len(effects) != 0 {
		return errors.New("lifecycle log unexpectedly produced monetary effects")
	}

	return nil
}

func position(audit uint64, order uint32, log uint64) balancehistory.Position {
	return balancehistory.Position{AuditSequence: audit, OrderIndex: order, LogSequence: log}
}

func createLedgerLog(sequence uint64, name string, id uint32) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{
			Name: name,
			Id:   id,
		}}},
	}
}

func deleteLedgerLog(sequence uint64, name string) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{
			Name: name,
		}}},
	}
}

func transactionLog(sequence uint64, ledger string, payload *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log:        &commonpb.LedgerLog{Id: sequence, Data: payload},
		}}},
	}
}

func createdTransaction(posting *commonpb.Posting, effective, inserted uint64) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
		CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction(posting, effective, inserted)},
	}}
}

func revertedTransaction(posting *commonpb.Posting, effective, inserted uint64) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
		RevertedTransaction: &commonpb.RevertedTransaction{RevertTransaction: transaction(posting, effective, inserted)},
	}}
}

func transaction(posting *commonpb.Posting, effective, inserted uint64) *commonpb.Transaction {
	return &commonpb.Transaction{
		Postings:   []*commonpb.Posting{posting},
		Timestamp:  &commonpb.Timestamp{Data: effective},
		InsertedAt: &commonpb.Timestamp{Data: inserted},
	}
}

func posting(source, destination, asset, color string, value uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Color:       color,
		Amount:      &commonpb.Uint256{V0: value},
	}
}

func amount(value uint64) balancehistory.Amount {
	var ret balancehistory.Amount
	for index := range 8 {
		ret[len(ret)-1-index] = byte(value >> (index * 8))
	}

	return ret
}
