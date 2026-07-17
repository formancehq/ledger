package numscript

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// TestStore_ServesColoredBalanceQuery pins that color IS a volume dimension: a
// color-qualified balance query resolves the segregated (account, asset, color)
// bucket and echoes the color back on the row.
func TestStore_ServesColoredBalanceQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withColoredBalance("acc", "COIN", "RED", 100).build(t), false)

	balances, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Color: "RED"},
	})
	require.NoError(t, err)
	require.Len(t, balances, 1)
	require.Equal(t, "RED", balances[0].Color)
	require.Equal(t, "100", balances[0].Amount.String())
}

// TestStore_RejectsScopedBalanceQuery pins that scope is NOT modelled: a
// scope-qualified balance query is rejected (serving each scope the full balance
// lets one script spend the same funds once per scope — double-spend).
func TestStore_RejectsScopedBalanceQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withBalance("acc", "COIN", 100).build(t), false)

	_, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Scope: "reserve"},
	})
	require.ErrorIs(t, err, domain.ErrScopedBalanceUnsupported)
}

// TestStore_ServesColoredBalanceInForceMode pins that a colored query in force
// mode returns MaxForceBalance (no rejection — force works for any color).
func TestStore_ServesColoredBalanceInForceMode(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().build(t), true)

	balances, err := store.GetBalances(context.Background(), numscriptlib.BalanceQuery{
		{Account: "acc", Asset: "COIN", Color: "RED"},
	})
	require.NoError(t, err)
	require.Len(t, balances, 1)
	require.Equal(t, "RED", balances[0].Color)
	require.Equal(t, MaxForceBalance.String(), balances[0].Amount.String())
}

// TestStore_RejectsScopedMetadataQuery pins the scope rejection for account
// metadata reads (metadata has no color dimension, only scope — still
// unsupported).
func TestStore_RejectsScopedMetadataQuery(t *testing.T) {
	t.Parallel()

	store := NewStore(newFakeSource().withMetadata("acc", "k", "v").build(t), false)

	_, err := store.GetAccountsMetadata(context.Background(), numscriptlib.MetadataQuery{
		{Account: "acc", Scope: "s", Keys: []string{"k"}},
	})
	require.ErrorIs(t, err, domain.ErrScopedBalanceUnsupported)
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

// TestStore_ScopedRejectionIsValidationError confirms the rejection is a
// validation-kind business error (non-retryable), so admission/FSM surface it as
// a client error rather than an internal one.
func TestStore_ScopedRejectionIsValidationError(t *testing.T) {
	t.Parallel()

	var d domain.Describable
	require.True(t, errors.As(domain.ErrScopedBalanceUnsupported, &d))
	require.Equal(t, domain.ErrReasonValidation, d.Reason())
}

// TestRecordingStore_HashInjectiveOverAmbiguousMetadata pins that the
// stale-inputs digest is injective even for attacker-controlled metadata values
// containing the former framing delimiters (`=` / `\n`). Under the old
// `key=value\n` text encoding, set A (one record whose value embeds the second
// record) and set B (two records) serialized to the identical byte stream and
// hashed the same — letting a crafted value evade stale detection. The
// length-delimited encoding must hash them distinctly.
func TestRecordingStore_HashInjectiveOverAmbiguousMetadata(t *testing.T) {
	t.Parallel()

	a := &RecordingStore{
		balanceRecords:  map[string]string{},
		metadataRecords: map[string]string{"acct\x00\x00k1": "v\nacct\x00\x00k2=w"},
	}
	b := &RecordingStore{
		balanceRecords:  map[string]string{},
		metadataRecords: map[string]string{"acct\x00\x00k1": "v", "acct\x00\x00k2": "w"},
	}

	require.NotEqual(t, a.Hash(), b.Hash(),
		"distinct metadata record sets that collided under key=value framing must hash distinctly")
}
