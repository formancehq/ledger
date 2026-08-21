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
			LedgerName: "default", AuditSequence: 2, OrderIndex: 3, LogSequence: 11,
			EffectiveAt: 100, InsertedAt: 200, Account: "users:001",
			AssetBase: "USD", AssetPrecision: 2, Color: "BLUE", Output: amount(42),
		},
		{
			LedgerName: "default", AuditSequence: 2, OrderIndex: 3, LogSequence: 11,
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
			require.Equal(t, "default", effects[0].LedgerName)
			require.Equal(t, "EUR", effects[0].AssetBase)
			require.Equal(t, uint8(4), effects[0].AssetPrecision)
			require.Equal(t, "OPS", effects[0].Color)
			require.Equal(t, uint64(300), effects[0].EffectiveAt)
			require.Equal(t, uint64(400), effects[0].InsertedAt)
		})
	}
}

func TestReducerRejectsImpossibleDeleteAndRecreateLifecycle(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 10, createLedgerLog(10, "default", 7)))
	_, err := reducer.Reduce(position(2, 0, 11), transactionLog(11, "default", createdTransaction(posting("world", "cash", "USD", "", 1), 10, 20)))
	require.NoError(t, err)
	require.NoError(t, reduceLifecycle(reducer, 3, 0, 12, deleteLedgerLog(12, "default")))
	err = reduceLifecycle(reducer, 4, 0, 13, createLedgerLog(13, "default", 99))
	require.ErrorIs(t, err, balancehistory.ErrInvalidLifecycle)
	require.ErrorContains(t, err, "was already used")
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

func TestReducerTracksHistoricalBalanceConfigurationAndProjectionSet(t *testing.T) {
	t.Parallel()

	reducer := balancehistory.NewReducer()
	require.NoError(t, reduceLifecycle(reducer, 1, 0, 1, createLedgerLog(1, "default", 7)))
	_, err := reducer.Reduce(position(2, 0, 2), transactionLog(2, "default", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_ConfiguredHistoricalBalances{
			ConfiguredHistoricalBalances: &commonpb.ConfiguredHistoricalBalancesLog{Enabled: true},
		},
	}))
	require.NoError(t, err)
	require.Equal(t, []string{"default"}, reducer.State().Enabled)

	reducer.SetProjectedLedgers(nil)
	effects, err := reducer.Reduce(position(3, 0, 3), transactionLog(3, "default", createdTransaction(posting("world", "cash", "USD", "", 1), 10, 20)))
	require.NoError(t, err)
	require.Empty(t, effects, "a rebuild with an empty final configuration must not emit account rows")

	_, err = reducer.Reduce(position(4, 0, 4), transactionLog(4, "default", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_ConfiguredHistoricalBalances{
			ConfiguredHistoricalBalances: &commonpb.ConfiguredHistoricalBalancesLog{Enabled: false},
		},
	}))
	require.NoError(t, err)
	require.Empty(t, reducer.State().Enabled)
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

func TestReducerRejectsMalformedLifecycleAndApplyLogs(t *testing.T) {
	t.Parallel()

	var nilReducer *balancehistory.Reducer
	_, err := nilReducer.Reduce(position(1, 0, 1), createLedgerLog(1, "default", 1))
	require.ErrorIs(t, err, balancehistory.ErrMalformedLog)
	require.ErrorContains(t, err, "nil reducer")

	tests := []struct {
		name    string
		prepare bool
		log     func() *commonpb.Log
		want    string
	}{
		{name: "nil log", log: func() *commonpb.Log { return nil }, want: "nil log"},
		{
			name: "missing payload",
			log:  func() *commonpb.Log { return &commonpb.Log{Sequence: 2} },
			want: "has no payload",
		},
		{
			name: "nil create",
			log: func() *commonpb.Log {
				return &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{}}}
			},
			want: "incomplete create-ledger log",
		},
		{
			name: "empty create name",
			log: func() *commonpb.Log {
				return createLedgerLog(2, "", 1)
			},
			want: "incomplete create-ledger log",
		},
		{
			name: "zero create id",
			log: func() *commonpb.Log {
				return createLedgerLog(2, "default", 0)
			},
			want: "incomplete create-ledger log",
		},
		{
			name:    "duplicate active ledger",
			prepare: true,
			log: func() *commonpb.Log {
				return createLedgerLog(2, "default", 8)
			},
			want: "already active",
		},
		{
			name: "nil delete",
			log: func() *commonpb.Log {
				return &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{}}}
			},
			want: "incomplete delete-ledger log",
		},
		{
			name: "inactive delete",
			log: func() *commonpb.Log {
				return deleteLedgerLog(2, "missing")
			},
			want: "deleting inactive ledger",
		},
		{
			name: "nil apply",
			log: func() *commonpb.Log {
				return &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{}}}
			},
			want: "incomplete apply log",
		},
		{
			name: "apply without ledger name",
			log: func() *commonpb.Log {
				return &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{}}},
				}}}
			},
			want: "incomplete apply log",
		},
		{
			name: "apply without ledger log",
			log: func() *commonpb.Log {
				return &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{LedgerName: "default"},
				}}}
			},
			want: "incomplete apply log",
		},
		{
			name: "apply without ledger payload",
			log: func() *commonpb.Log {
				return transactionLog(2, "default", &commonpb.LedgerLogPayload{})
			},
			want: "apply log has no ledger payload",
		},
		{
			name:    "nil created transaction",
			prepare: true,
			log: func() *commonpb.Log {
				return transactionLog(2, "default", &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreatedTransaction{},
				})
			},
			want: "nil created-transaction payload",
		},
		{
			name:    "nil reverted transaction",
			prepare: true,
			log: func() *commonpb.Log {
				return transactionLog(2, "default", &commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_RevertedTransaction{},
				})
			},
			want: "nil reverted-transaction payload",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reducer := balancehistory.NewReducer()
			if test.prepare {
				require.NoError(t, reduceLifecycle(reducer, 1, 0, 1, createLedgerLog(1, "default", 7)))
			}
			_, err := reducer.Reduce(position(2, 0, 2), test.log())
			require.ErrorContains(t, err, test.want)
		})
	}

	reducer := balancehistory.NewReducer()
	_, err = reducer.Reduce(position(0, 0, 1), createLedgerLog(1, "default", 1))
	require.ErrorIs(t, err, balancehistory.ErrInvalidPosition)
	_, err = reducer.Reduce(position(1, 0, 0), createLedgerLog(1, "default", 1))
	require.ErrorIs(t, err, balancehistory.ErrInvalidPosition)
}

func TestReducerRejectsMalformedTransactionsAndPostings(t *testing.T) {
	t.Parallel()

	validTransaction := func(postings ...*commonpb.Posting) *commonpb.Transaction {
		return &commonpb.Transaction{
			Postings:   postings,
			Timestamp:  &commonpb.Timestamp{Data: 10},
			InsertedAt: &commonpb.Timestamp{Data: 20},
		}
	}
	created := func(transaction *commonpb.Transaction) *commonpb.Log {
		return transactionLog(2, "default", &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction},
			},
		})
	}
	tests := []struct {
		name string
		log  func() *commonpb.Log
		want string
	}{
		{name: "nil transaction", log: func() *commonpb.Log { return created(nil) }, want: "missing its effective or insertion timestamp"},
		{
			name: "missing effective timestamp",
			log: func() *commonpb.Log {
				return created(&commonpb.Transaction{InsertedAt: &commonpb.Timestamp{Data: 20}})
			},
			want: "missing its effective or insertion timestamp",
		},
		{
			name: "missing insertion timestamp",
			log: func() *commonpb.Log {
				return created(&commonpb.Transaction{Timestamp: &commonpb.Timestamp{Data: 10}})
			},
			want: "missing its effective or insertion timestamp",
		},
		{name: "no postings", log: func() *commonpb.Log { return created(validTransaction()) }, want: "transaction has no resolved posting"},
		{name: "nil posting", log: func() *commonpb.Log { return created(validTransaction(nil)) }, want: "incomplete resolved posting"},
		{
			name: "missing source",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("", "cash", "USD", "", 1)))
			},
			want: "incomplete resolved posting",
		},
		{
			name: "missing destination",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("world", "", "USD", "", 1)))
			},
			want: "incomplete resolved posting",
		},
		{
			name: "nil amount",
			log: func() *commonpb.Log {
				value := posting("world", "cash", "USD", "", 1)
				value.Amount = nil

				return created(validTransaction(value))
			},
			want: "incomplete resolved posting",
		},
		{
			name: "invalid source",
			log: func() *commonpb.Log {
				return created(validTransaction(posting(":", "cash", "USD", "", 1)))
			},
			want: "invalid source account",
		},
		{
			name: "invalid destination",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("world", ":", "USD", "", 1)))
			},
			want: "invalid destination account",
		},
		{
			name: "invalid asset",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("world", "cash", "USD/", "", 1)))
			},
			want: "invalid asset",
		},
		{
			name: "invalid color",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("world", "cash", "USD", "invalid color", 1)))
			},
			want: "invalid color",
		},
		{
			name: "zero amount",
			log: func() *commonpb.Log {
				return created(validTransaction(posting("world", "cash", "USD", "", 0)))
			},
			want: "resolved posting amount is zero",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reducer := balancehistory.NewReducer()
			require.NoError(t, reduceLifecycle(reducer, 1, 0, 1, createLedgerLog(1, "default", 7)))
			_, err := reducer.Reduce(position(2, 0, 2), test.log())
			require.ErrorContains(t, err, test.want)
			require.ErrorIs(t, err, balancehistory.ErrMalformedLog)
		})
	}
}

func TestAmountHelpers(t *testing.T) {
	t.Parallel()

	require.True(t, balancehistory.AmountFromProto(nil).IsZero())
	amount := balancehistory.AmountFromUint64(42)
	require.False(t, amount.IsZero())
	require.Equal(t, "42", amount.BigInt().String())
	require.Equal(t, "42", balancehistory.AmountFromProto(&commonpb.Uint256{V0: 42}).BigInt().String())
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
