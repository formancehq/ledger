package ledger

import (
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/stretchr/testify/require"
)

func TestTransactionsReverse(t *testing.T) {
	tx := NewTransaction().
		WithPostings(
			NewPosting("world", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "payments:001", "COIN", big.NewInt(100)),
		)

	expected := NewTransaction().
		WithPostings(
			NewPosting("payments:001", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "world", "COIN", big.NewInt(100)),
		).
		WithTimestamp(tx.Timestamp)

	reversed := tx.Reverse()
	reversed.Timestamp = time.Time{}
	expected.Timestamp = time.Time{}
	require.Equal(t, expected, reversed)
}

func TestTransactionsVolumesUpdate(t *testing.T) {
	tx := NewTransaction().
		WithPostings(
			NewPosting("world", "users:001", "COIN", big.NewInt(100)),
			NewPosting("users:001", "payments:001", "COIN", big.NewInt(100)),
			NewPosting("payments:001", "world", "COIN", big.NewInt(100)),
		)

	require.Equal(t, []AccountsVolumes{
		{
			Account: "world",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
		{
			Account: "users:001",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
		{
			Account: "payments:001",
			Asset:   "COIN",
			Input:   big.NewInt(100),
			Output:  big.NewInt(100),
		},
	}, tx.VolumeUpdates())
}
