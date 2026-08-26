package ledger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"

	ledger "github.com/formancehq/ledger/internal"
)

func TestControllerWithEventsLockLedgerPreservesTransaction(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		commit bool
	}{
		{name: "commit publishes the event", commit: true},
		{name: "rollback discards the event", commit: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockController := gomock.NewController(t)
			root := NewMockController(mockController)
			transaction := NewMockController(mockController)
			locked := NewMockController(mockController)
			listener := NewMockListener(mockController)
			ctx := context.Background()
			parameters := Parameters[CreateTransaction]{}
			created := &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}
			eventPublished := false

			root.EXPECT().
				BeginTX(ctx, nil).
				Return(transaction, &bun.Tx{}, nil)
			transaction.EXPECT().
				LockLedger(ctx).
				Return(locked, nil, func() error { return nil }, nil)
			locked.EXPECT().
				CreateTransaction(ctx, parameters).
				Return(&ledger.Log{}, created, false, nil)

			if testCase.commit {
				transaction.EXPECT().Commit(ctx).Return(nil)
				listener.EXPECT().
					CommittedTransactions(ctx, "foo", created.Transaction, created.AccountMetadata).
					Do(func(context.Context, string, ledger.Transaction, ledger.AccountMetadata) {
						eventPublished = true
					})
			} else {
				transaction.EXPECT().Rollback(ctx).Return(nil)
			}

			controller := NewControllerWithEvents(ledger.Ledger{Name: "foo"}, root, listener)
			transactionController, _, err := controller.BeginTX(ctx, nil)
			require.NoError(t, err)

			lockedController, _, release, err := transactionController.LockLedger(ctx)
			require.NoError(t, err)

			_, _, _, err = lockedController.CreateTransaction(ctx, parameters)
			require.NoError(t, err)
			require.NoError(t, release())
			require.False(t, eventPublished)

			if testCase.commit {
				require.NoError(t, transactionController.Commit(ctx))
				require.True(t, eventPublished)
			} else {
				require.NoError(t, transactionController.Rollback(ctx))
				require.False(t, eventPublished)
			}
		})
	}
}

func TestControllerWithEventsNestedTransactions(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		commitNested bool
		commitOuter  bool
		publishEvent bool
	}{
		{name: "committing both transactions publishes the event", commitNested: true, commitOuter: true, publishEvent: true},
		{name: "rolling back the nested transaction discards the event", commitNested: false, commitOuter: true},
		{name: "rolling back the outer transaction discards a committed nested event", commitNested: true, commitOuter: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			mockController := gomock.NewController(t)
			root := NewMockController(mockController)
			outer := NewMockController(mockController)
			nested := NewMockController(mockController)
			listener := NewMockListener(mockController)
			ctx := context.Background()
			parameters := Parameters[CreateTransaction]{}
			created := &ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}
			eventPublished := false

			root.EXPECT().
				BeginTX(ctx, nil).
				Return(outer, &bun.Tx{}, nil)
			outer.EXPECT().
				BeginTX(ctx, nil).
				Return(nested, &bun.Tx{}, nil)
			nested.EXPECT().
				CreateTransaction(ctx, parameters).
				Return(&ledger.Log{}, created, false, nil)

			if testCase.commitNested {
				nested.EXPECT().Commit(ctx).Return(nil)
			} else {
				nested.EXPECT().Rollback(ctx).Return(nil)
			}
			if testCase.commitOuter {
				outer.EXPECT().Commit(ctx).Return(nil)
			} else {
				outer.EXPECT().Rollback(ctx).Return(nil)
			}
			if testCase.publishEvent {
				listener.EXPECT().
					CommittedTransactions(ctx, "foo", created.Transaction, created.AccountMetadata).
					Do(func(context.Context, string, ledger.Transaction, ledger.AccountMetadata) {
						eventPublished = true
					})
			}

			controller := NewControllerWithEvents(ledger.Ledger{Name: "foo"}, root, listener)
			outerController, _, err := controller.BeginTX(ctx, nil)
			require.NoError(t, err)
			nestedController, _, err := outerController.BeginTX(ctx, nil)
			require.NoError(t, err)

			_, _, _, err = nestedController.CreateTransaction(ctx, parameters)
			require.NoError(t, err)
			require.False(t, eventPublished)

			if testCase.commitNested {
				require.NoError(t, nestedController.Commit(ctx))
			} else {
				require.NoError(t, nestedController.Rollback(ctx))
			}
			require.False(t, eventPublished)

			if testCase.commitOuter {
				require.NoError(t, outerController.Commit(ctx))
			} else {
				require.NoError(t, outerController.Rollback(ctx))
			}
			require.Equal(t, testCase.publishEvent, eventPublished)
		})
	}
}
