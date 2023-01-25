package processor

import (
	"testing"

	"github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/orchestration/internal/activities"
	"github.com/formancehq/orchestration/internal/spec"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestProcessorPayoutFromWallet(t *testing.T) {

	// Set up the test suite and testing execution environment
	testSuite := &testsuite.WorkflowTestSuite{}
	walletID := uuid.NewString()
	stripeConnectID := uuid.NewString()
	holdID := uuid.NewString()

	env := testSuite.NewTestWorkflowEnvironment()
	env.
		OnActivity(activities.GetWalletActivity, mock.Anything, walletID).
		Return(&formance.GetWalletResponse{
			Data: formance.WalletWithBalances{
				Id: walletID,
				Metadata: map[string]interface{}{
					stripeConnectIDMetadata: stripeConnectID,
				},
			},
		}, nil)
	env.
		OnActivity(activities.DebitWalletActivity, mock.Anything, walletID, formance.DebitWalletRequest{
			Amount: formance.Monetary{
				Asset:  "USD",
				Amount: 1000,
			},
			Pending:  formance.PtrBool(true),
			Balances: []string{"main"},
		}).
		Return(&formance.DebitWalletResponse{
			Data: formance.Hold{
				Id:       holdID,
				WalletID: walletID,
			},
		}, nil)
	env.
		OnActivity(activities.StripeTransferActivity, mock.Anything, formance.StripeTransferRequest{
			Amount:      formance.PtrInt64(1000),
			Asset:       formance.PtrString("USD"),
			Destination: formance.PtrString(stripeConnectID),
		}).
		Return(nil)
	env.
		OnActivity(activities.ConfirmHoldActivity, mock.Anything, holdID).
		Return(nil)

	env.ExecuteWorkflow(Payout, Input{
		Specification: spec.Payout,
		Parameters: map[string]any{
			"source": map[string]any{
				"kind":   "wallet",
				"wallet": walletID,
			},
			"amount": map[string]any{
				"asset":  "USD",
				"amount": 1000,
			},
		},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
