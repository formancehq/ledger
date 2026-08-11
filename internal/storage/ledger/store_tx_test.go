//go:build it

package ledger_test

// store_tx_test.go covers the transaction-lifecycle surface of Store and the
// ResolveFilter fallthrough branches, all of which were previously untested.

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"

	"github.com/formancehq/ledger/internal/storage/common"
)

// TestStore_CommitRollbackOutsideTransaction verifies that Commit and Rollback report a
// clear error when the store is not transactional, rather than acting on the pool.
func TestStore_CommitRollbackOutsideTransaction(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	require.ErrorContains(t, store.Commit(ctx), "not in a transaction")
	require.ErrorContains(t, store.Rollback(ctx), "not in a transaction")
}

// TestStore_BeginTXCommit verifies the transactional store returned by BeginTX commits,
// and that Commit/Rollback dispatch on the transactional branch rather than erroring.
func TestStore_BeginTXCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	txStore, tx, err := store.BeginTX(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	require.NoError(t, txStore.Commit(ctx))
	// Committing twice must fail rather than silently succeed.
	require.Error(t, txStore.Commit(ctx))
}

// TestStore_BeginTXRollback covers the Rollback path on a transactional store.
func TestStore_BeginTXRollback(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	txStore, _, err := store.BeginTX(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, txStore.Rollback(ctx))
}

// TestStore_GetBucket verifies GetBucket returns a bucket that is actually usable —
// newLedgerStore has migrated it, so it must report itself up to date.
func TestStore_GetBucket(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	b := store.GetBucket()
	require.NotNil(t, b)

	upToDate, err := b.IsUpToDate(ctx, store.GetDB())
	require.NoError(t, err)
	require.True(t, upToDate, "bucket returned by GetBucket must be the migrated one")
}

// TestTransactionsResolveFilter_Fallthrough covers the two ResolveFilter branches reached
// by properties the transaction schema accepts but the filter builder does not translate
// to a metadata-key predicate.
func TestTransactionsResolveFilter_Fallthrough(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	store := newLedgerStore(t)

	t.Run("bare metadata property uses key-existence", func(t *testing.T) {
		// "metadata" (no [key]) does not match MetadataRegex, so it falls through to the
		// `metadata -> ? is not null` branch.
		_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
			Options: common.ResourceQuery[any]{
				Builder: query.Match("metadata", "some_key"),
			},
		})
		require.NoError(t, err)
	})

	// $in requires full addresses. "users:" has an empty trailing segment, so it is a
	// partial address: it passes the schema's string-array validation and is then
	// rejected by the filter builder. One case per address property, since each has its
	// own branch.
	for _, property := range []string{"account", "source", "destination"} {
		t.Run("$in rejects a partial address on "+property, func(t *testing.T) {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				Options: common.ResourceQuery[any]{
					Builder: query.In(property, []any{"users:"}),
				},
			})
			require.ErrorContains(t, err, "IN operator only supports full addresses")
		})

		t.Run("$in accepts full addresses on "+property, func(t *testing.T) {
			_, err := store.Transactions().Paginate(ctx, common.InitialPaginatedQuery[any]{
				Options: common.ResourceQuery[any]{
					Builder: query.In(property, []any{"users:alice", "users:bob"}),
				},
			})
			require.NoError(t, err)
		})
	}
}
