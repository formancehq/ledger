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

func TestProcessorPayinFromWallet(t *testing.T) {

	// Set up the test suite and testing execution environment
	testSuite := &testsuite.WorkflowTestSuite{}
	paymentID := uuid.NewString()
	walletID := uuid.NewString()

	env := testSuite.NewTestWorkflowEnvironment()
	env.
		OnActivity(activities.GetPaymentActivity, mock.Anything, paymentID).
		Return(&formance.PaymentResponse{
			Data: formance.Payment{
				InitialAmount: 100,
				Asset:         "USD",
				Provider:      formance.STRIPE,
				Status:        formance.TERMINATED,
			},
		}, nil)
	env.
		OnActivity(activities.GetWalletActivity, mock.Anything, walletID).
		Return(&formance.GetWalletResponse{
			Data: formance.WalletWithBalances{
				Id:     walletID,
				Ledger: "default",
			},
		}, nil)
	env.
		OnActivity(activities.CreateTransactionActivity, mock.Anything, "default", formance.PostTransaction{
			Postings: []formance.Posting{{
				Amount:      100,
				Asset:       "USD",
				Destination: "orchestration:payins:" + paymentID,
				Source:      "world",
			}},
		}).
		Return(&formance.TransactionsResponse{
			Data: []formance.Transaction{{
				Txid: 0,
			}},
		}, nil)
	env.
		OnActivity(activities.CreditWalletActivity, mock.Anything, walletID, formance.CreditWalletRequest{
			Amount: formance.Monetary{
				Asset:  "USD",
				Amount: 100,
			},
			Sources: []formance.Subject{{
				LedgerAccountSubject: formance.NewLedgerAccountSubject("LEDGER", "orchestration:payins:"+paymentID),
			}},
			Balance: formance.PtrString("main"),
		}).
		Return(nil)

	env.ExecuteWorkflow(Payin, Input{
		Specification: spec.Payin,
		Parameters: map[string]any{
			"source": paymentID,
			"destination": map[string]any{
				"kind":   "wallet",
				"wallet": walletID,
			},
		},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
