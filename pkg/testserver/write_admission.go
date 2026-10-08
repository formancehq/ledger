package testserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// WaitForWriteAdmission waits for a new leader's first disk-health verdict.
// A non-empty batch with an invalid ledger name reaches the write gate but
// cannot mutate state once admission is ready.
func WaitForWriteAdmission(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient) {
	t.Helper()
	request := servicepb.UnsignedApplyRequest("write-gate-probe", actions.CreateLedgerAction("", nil))
	require.Eventually(t, func() bool {
		_, err := client.Apply(ctx, request)

		return status.Code(err) == codes.InvalidArgument
	}, 5*time.Second, 10*time.Millisecond, "write admission did not become ready")
}
