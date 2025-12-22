//go:build it
// +build it

package service

import (
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// createTestLogs creates a set of test logs with different types
func createTestLogs(t *testing.T, ledgerName string) []*ledgerpb.Log {
	now := libtime.New(time.Now())

	logs := []*ledgerpb.Log{
		// Log 1: CreatedTransaction
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
					).
					WithID(1).
					WithTimestamp(now),
				AccountMetadata: map[string]*ledgerpb.Metadata{
					"bank": {Entries: metadata.Metadata{
						"account_type": "asset",
					}},
				},
			})
			return ledgerpb.NewLog(payload).
				WithID(1).
				WithSequence(1).
				WithIdempotencyKey("idempotency-key-1").
				WithDate(now)
		}(),

		// Log 2: CreatedTransaction with different idempotency key
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
					).
					WithID(2).
					WithTimestamp(now),
			})
			return ledgerpb.NewLog(payload).
				WithID(2).
				WithSequence(2).
				WithIdempotencyKey("idempotency-key-2").
				WithDate(now.Add(time.Second))
		}(),

		// Log 3: SavedMetadata
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.SavedMetadata{
				TargetType: "ACCOUNT",
				TargetId:   &ledgerpb.SavedMetadata_AccountId{AccountId: "bank"},
				Metadata: metadata.Metadata{
					"label": "Bank Account",
				},
			})
			return ledgerpb.NewLog(payload).
				WithID(3).
				WithSequence(3).
				WithDate(now.Add(2 * time.Second))
		}(),

		// Log 4: DeletedMetadata
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.DeletedMetadata{
				TargetType: "ACCOUNT",
				TargetId:   &ledgerpb.DeletedMetadata_AccountId{AccountId: "bank"},
				Key:        "old_key",
			})
			return ledgerpb.NewLog(payload).
				WithID(4).
				WithSequence(4).
				WithDate(now.Add(3 * time.Second))
		}(),

		// Log 5: RevertedTransaction
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.RevertedTransaction{
				RevertedTransaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
					).
					WithID(1).
					WithTimestamp(now),
				RevertTransaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("bank", "world", "USD", big.NewInt(100)),
					).
					WithID(5).
					WithTimestamp(now.Add(4 * time.Second)),
			})
			return ledgerpb.NewLog(payload).
				WithID(5).
				WithSequence(5).
				WithDate(now.Add(4 * time.Second))
		}(),
	}

	return logs
}
