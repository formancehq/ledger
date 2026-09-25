package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// TestRevertOfSameBatchTargetAgainstServer pins the server outcome for a bulk
// that reverts a transaction it also creates, and that the model predicts the
// same reason.
//
// Admission resolves a revert's original postings from the local store, and the
// bulk overlay does not carry transactions the batch itself creates, so it
// cannot declare the volume coverage apply needs. Before this was classified,
// the server answered COVERAGE_MISS — an Internal error the client does not
// retry, and which the coverage gate documents as an admission bug rather than
// a caller mistake. The model meanwhile predicted success, so no test noticed.
func TestRevertOfSameBatchTargetAgainstServer(t *testing.T) {
	t.Parallel()

	ctx, client := skippableTestServer(t)

	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)

	reqs := []*servicepb.Request{
		oracletest.TxReq("world", "acc:1", "USD", 5),
		oracletest.RevertReqL("L", 1, true),
	}

	predicted := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.False(t, predicted.OK, "the model must not predict success for a batch the server rejects")
	require.Equal(t, domain.ErrReasonRevertTargetCreatedInBatch, predicted.Reason)

	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
	require.Error(t, err)
	require.Equal(t, predicted.Reason, internal.ErrorReason(err),
		"the server and the model must agree on the reason")

	// The same revert in a later bulk is ordinary and commits.
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", oracletest.TxReq("world", "acc:1", "USD", 5)))
	require.NoError(t, err)

	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", oracletest.RevertReqL("L", 1, true)))
	require.NoError(t, err)
}
