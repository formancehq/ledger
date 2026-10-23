package internal

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestSentinelRealServerContent(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	// CheckEmissions parallelizes only the parent; StartServer sets environment
	// variables in the isolated child and therefore cannot run under t.Parallel.
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		ledger := PrefixSentinel.WithSuffix("content-smoke")
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		require.NoError(t, err)
		first, err := PreCommitSentinel(ctx, client, ledger)
		require.NoError(t, err)
		// A second write changes the shared world balance. Historical post-commit
		// volumes in the first transaction must still equal its acknowledged ones.
		_, err = PreCommitSentinel(ctx, client, ledger)
		require.NoError(t, err)
		response, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: first.TxID})
		require.NoError(t, err)
		require.True(t, proto.Equal(first.expected, response.GetTransaction()))
		require.NotEmpty(t, response.GetTransaction().GetMetadata())
		first.Verify(ctx, client, "after-another-commit")
	}, func(records []drivertest.Assertion) {
		contentHit, successHit := false, false
		for _, record := range records {
			if !record.Hit {
				continue
			}
			switch record.Message {
			case sentinelContentProperty:
				contentHit = true
				require.True(t, record.Condition, "content mismatch: %+v", record)
			case "sentinel transaction read-after-write succeeded":
				successHit = true
				require.True(t, record.Condition)
			}
		}
		require.True(t, contentHit, "real server must evaluate exact content equality")
		require.True(t, successHit, "real server must complete sentinel verification")
	})
}
