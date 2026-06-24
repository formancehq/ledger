package internal

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel records a committed transaction whose survival across an operational
// disruption (rolling restart, config change, quorum loss + force-remove) is
// asserted by drivers via Verify(). The Reference is captured for debugging
// (it appears in assertion Details) but lookup goes through the TxID.
type Sentinel struct {
	Ledger    string
	Reference string
	TxID      uint64
}

// PreCommitSentinel commits a deterministic-looking transaction with a unique
// reference and returns a Sentinel that can be re-verified after a disruption.
// The transaction uses `world -> sentinel:<uniq>` with a fixed amount so it
// never depends on prior state, and so it doesn't interfere with other drivers
// that touch `users:N`.
func PreCommitSentinel(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (*Sentinel, error) {
	ref := fmt.Sprintf("sentinel-%d-%d", Rand().Uint64(), Rand().Uint64())
	destination := fmt.Sprintf("sentinel:%d", Rand().Uint64())

	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:      []*commonpb.Posting{commonpb.NewPosting("world", destination, "COIN", RandomBigInt())},
						Reference:     ref,
						Force:         true,
						ExpandVolumes: true,
					},
				}},
			},
		},
	}))
	if err != nil {
		return nil, err
	}

	createdTx := ExtractCreatedTransaction(resp)
	if createdTx == nil {
		return nil, fmt.Errorf("apply returned no CreatedTransaction")
	}

	return &Sentinel{
		Ledger:    ledger,
		Reference: ref,
		TxID:      createdTx.Transaction.Id,
	}, nil
}

// Verify asserts the sentinel transaction is still readable via the gRPC
// client. Transient failures (UNAVAILABLE, ledger-deleted, etc.) are downgraded
// to a Reachable check; a NotFound on a previously committed transaction is a
// hard failure (Always violation).
func (s *Sentinel) Verify(ctx context.Context, client servicepb.BucketServiceClient, label string) {
	details := Details{
		"label":     label,
		"ledger":    s.Ledger,
		"reference": s.Reference,
		"txId":      s.TxID,
	}

	_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        s.Ledger,
		TransactionId: s.TxID,
	})
	if err == nil {
		assert.Reachable("sentinel transaction read-after-write succeeded", details)
		return
	}
	if IsTransient(err) {
		assert.Reachable("sentinel verify hit a transient error", details.With(Details{"error": err}))
		return
	}
	st, _ := status.FromError(err)
	// A committed sentinel must never be NotFound, regardless of the operational
	// disruption that occurred between PreCommit and Verify.
	assert.Always(st.Code() != codes.NotFound, "committed sentinel transaction must survive operational events", details.With(Details{"error": err}))
}
