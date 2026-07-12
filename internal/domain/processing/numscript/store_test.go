package numscript

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// TestStore_RejectsColoredBalanceQuery pins EN-1406 P1-2: Ledger volumes have no
// color dimension, so a color-qualified balance query must be rejected — serving
// each color view the full balance lets one script spend the same funds once per
// color (double-spend).
func TestStore_RejectsColoredBalanceQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withBalance("acc", "COIN", 100).build(t), false)

	_, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Color: "RED"},
	})
	require.ErrorIs(t, err, domain.ErrColoredBalanceUnsupported)
}

// TestStore_RejectsScopedBalanceQuery pins the same for a scope-qualified query.
func TestStore_RejectsScopedBalanceQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withBalance("acc", "COIN", 100).build(t), false)

	_, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Scope: "reserve"},
	})
	require.ErrorIs(t, err, domain.ErrColoredBalanceUnsupported)
}

// TestStore_RejectsColoredBalanceEvenInForceMode pins that force mode does not
// bypass the rejection — force skips the balance CHECK, but a colored view is
// still semantically unsound.
func TestStore_RejectsColoredBalanceEvenInForceMode(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().build(t), true)

	_, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Color: "RED"},
	})
	require.ErrorIs(t, err, domain.ErrColoredBalanceUnsupported)
}

// TestStore_RejectsScopedMetadataQuery pins the same for scope-qualified account
// metadata reads.
func TestStore_RejectsScopedMetadataQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withMetadata("acc", "k", "v").build(t), false)

	_, err := store.GetAccountsMetadata(context.Background(), numscriptlib.MetadataQuery{
		{Account: "acc", Scope: "s", Keys: []string{"k"}},
	})
	require.ErrorIs(t, err, domain.ErrColoredBalanceUnsupported)
}

// TestStore_AllowsUncoloredQueries confirms the common (uncolored, unscoped)
// path still works.
func TestStore_AllowsUncoloredQueries(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withBalance("acc", "COIN", 100).withMetadata("acc", "k", "v").build(t), false)

	balances, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN"},
	})
	require.NoError(t, err)
	require.Len(t, balances, 1)
	require.Equal(t, "100", balances[0].Amount.String())

	metas, err := store.GetAccountsMetadata(context.Background(), numscriptlib.MetadataQuery{
		{Account: "acc", Keys: []string{"k"}},
	})
	require.NoError(t, err)
	require.Len(t, metas, 1)
	require.Equal(t, "v", metas[0].Value)
}

// TestStore_ColoredRejectionIsValidationError confirms the rejection is a
// validation-kind business error (non-retryable), so admission/FSM surface it as
// a client error rather than an internal one.
func TestStore_ColoredRejectionIsValidationError(t *testing.T) {
	t.Parallel()

	var d domain.Describable
	require.True(t, errors.As(domain.ErrColoredBalanceUnsupported, &d))
	require.Equal(t, domain.ErrReasonValidation, d.Reason())
}
