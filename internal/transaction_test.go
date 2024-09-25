package ledger

import (
	"github.com/formancehq/go-libs/time"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReverseTransaction(t *testing.T) {
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

	reversed := tx.Reverse(true)
	reversed.InsertedAt = time.Time{}
	expected.InsertedAt = time.Time{}
	require.Equal(t, expected, reversed)
}
