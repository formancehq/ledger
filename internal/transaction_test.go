package ledger

import (
	"math/big"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestReverseTransaction(t *testing.T) {
	t.Run("1 posting", func(t *testing.T) {
		tx := &ExpandedTransaction{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: Postings{
						{
							Source:      "world",
							Destination: "users:001",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
					},
					Reference: "foo",
				},
			},
		}

		expected := TransactionData{
			Postings: Postings{
				{
					Source:      "users:001",
					Destination: "world",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
			},
		}
		require.Equal(t, expected, tx.Reverse())
	})

	t.Run("2 postings", func(t *testing.T) {
		tx := &ExpandedTransaction{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: Postings{
						{
							Source:      "world",
							Destination: "users:001",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
						{
							Source:      "users:001",
							Destination: "payments:001",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
					},
					Reference: "foo",
				},
			},
		}

		expected := TransactionData{
			Postings: Postings{
				{
					Source:      "payments:001",
					Destination: "users:001",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
				{
					Source:      "users:001",
					Destination: "world",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
			},
		}
		require.Equal(t, expected, tx.Reverse())
	})

	t.Run("3 postings", func(t *testing.T) {
		tx := &ExpandedTransaction{
			Transaction: Transaction{
				TransactionData: TransactionData{
					Postings: Postings{
						{
							Source:      "world",
							Destination: "users:001",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
						{
							Source:      "users:001",
							Destination: "payments:001",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
						{
							Source:      "payments:001",
							Destination: "alice",
							Amount:      big.NewInt(100),
							Asset:       "COIN",
						},
					},
					Reference: "foo",
				},
			},
		}

		expected := TransactionData{
			Postings: Postings{
				{
					Source:      "alice",
					Destination: "payments:001",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
				{
					Source:      "payments:001",
					Destination: "users:001",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
				{
					Source:      "users:001",
					Destination: "world",
					Amount:      big.NewInt(100),
					Asset:       "COIN",
				},
			},
		}
		require.Equal(t, expected, tx.Reverse())
	})
}

func BenchmarkHash(b *testing.B) {
	logs := make([]ChainedLog, b.N)
	var previous *ChainedLog
	for i := 0; i < b.N; i++ {
		newLog := NewTransactionLog(NewTransaction().WithPostings(
			NewPosting("world", "bank", "USD", big.NewInt(100)),
		), map[string]metadata.Metadata{}).ChainLog(previous)
		previous = newLog
		logs = append(logs, *newLog)
	}

	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		logs[i].ComputeHash(&logs[i-1])
	}
}
