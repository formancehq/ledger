//go:build it
// +build it

package service

import (
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// createTestLogs creates a set of test logs with different types
func createTestLogs(t *testing.T, ledgerName string) []ledger.Log {
	now := libtime.New(time.Now())

	logs := []ledger.Log{
		// Log 1: CreatedTransaction
		ledger.NewLog(&ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				).
				WithID(1).
				WithTimestamp(now),
			AccountMetadata: ledger.AccountMetadata{
				"bank": metadata.Metadata{
					"account_type": "asset",
				},
			},
		}).
			WithID(1).
			WithSequence(1).
			WithIdempotencyKey("idempotency-key-1").
			WithDate(now),

		// Log 2: CreatedTransaction with different idempotency key
		ledger.NewLog(&ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("bank", "user", "USD", big.NewInt(50)),
				).
				WithID(2).
				WithTimestamp(now),
		}).
			WithID(2).
			WithSequence(2).
			WithIdempotencyKey("idempotency-key-2").
			WithDate(now.Add(time.Second)),

		// Log 3: SavedMetadata
		ledger.NewLog(&ledger.SavedMetadata{
			TargetType: "ACCOUNT",
			TargetID:   "bank",
			Metadata: metadata.Metadata{
				"label": "Bank Account",
			},
		}).
			WithID(3).
			WithSequence(3).
			WithDate(now.Add(2 * time.Second)),

		// Log 4: DeletedMetadata
		ledger.NewLog(&ledger.DeletedMetadata{
			TargetType: "ACCOUNT",
			TargetID:   "bank",
			Key:        "old_key",
		}).
			WithID(4).
			WithSequence(4).
			WithDate(now.Add(3 * time.Second)),

		// Log 5: RevertedTransaction
		ledger.NewLog(&ledger.RevertedTransaction{
			RevertedTransaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				).
				WithID(1).
				WithTimestamp(now),
			RevertTransaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("bank", "world", "USD", big.NewInt(100)),
				).
				WithID(5).
				WithTimestamp(now.Add(4 * time.Second)),
		}).
			WithID(5).
			WithSequence(5).
			WithDate(now.Add(4 * time.Second)),
	}

	return logs
}
