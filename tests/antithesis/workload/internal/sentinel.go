package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
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

// PreCommitSentinel commits a transaction with a unique reference and a
// Hegel-generated amount, and returns a Sentinel to verify after a disruption.
// The transaction uses `world -> sentinel:<uniq>` with Force so it
// never depends on prior state, and so it doesn't interfere with other drivers
// that touch `users:N`.
func PreCommitSentinel(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (*Sentinel, error) {
	// Keep technical identities on Antithesis entropy to avoid Hegel boundary
	// bias causing reference collisions across shared-cluster episodes.
	ref := fmt.Sprintf("sentinel-%d-%d", Rand().Uint64(), Rand().Uint64())
	destination := fmt.Sprintf("sentinel:%d", Rand().Uint64())

	var posting *commonpb.Posting
	if err := RunOperationalGeneration("sentinel", func(src *generation.Source) {
		posting = commonpb.NewPosting("world", destination, "COIN", GeneratedBigInt(src))
	}); err != nil {
		return nil, fmt.Errorf("generate sentinel: %w", err)
	}

	// Only values leave the callback; generation cannot replay the RPC.
	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:  []*commonpb.Posting{posting},
						Reference: ref,
						Force:     true,
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
		return nil, errors.New("apply returned no CreatedTransaction")
	}

	return &Sentinel{
		Ledger:    ledger,
		Reference: ref,
		TxID:      createdTx.GetTransaction().GetId(),
	}, nil
}

// Verify asserts the sentinel transaction is still readable via the gRPC
// client. Transient failures are recorded separately; a NotFound on a previously
// committed transaction is a forbidden outcome. Successful reads have their own
// required coverage, so the failure-only observation must not require a hit.
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
	if st.Code() == codes.NotFound {
		assert.Unreachable("committed sentinel transaction must survive operational events", details.With(Details{"error": err}))
	}
}
