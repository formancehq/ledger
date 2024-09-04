//go:build it

package performance_test

import (
	"context"
	formance "github.com/formancehq/formance-sdk-go/v2"
	"github.com/formancehq/formance-sdk-go/v2/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v2/pkg/models/shared"
	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

type RemoteStackEnvFactory struct {
	httpClient *http.Client
	stackURL   string
}

func (r *RemoteStackEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

	client := formance.New(
		formance.WithClient(r.httpClient),
		formance.WithServerURL(r.stackURL),
	)

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		V2CreateLedgerRequest: &shared.V2CreateLedgerRequest{
			Bucket:   &ledger.Bucket,
			Metadata: ledger.Metadata,
			Features: ledger.Features,
		},
		Ledger: ledger.Name,
	})
	require.NoError(b, err)

	return NewRemoveStackEnv(client, ledger)
}

var _ EnvFactory = (*RemoteStackEnvFactory)(nil)

func NewRemoteStackEnvFactory(httpClient *http.Client, stackURL string) *RemoteStackEnvFactory {
	return &RemoteStackEnvFactory{
		httpClient: httpClient,
		stackURL:   stackURL,
	}
}

type RemoteStackEnv struct {
	ledger ledger.Ledger
	client *formance.Formance
}

func (r *RemoteStackEnv) Executor() TransactionExecutor {
	return TransactionExecutorFn(func(ctx context.Context, script string, vars map[string]string) (*ledger.Transaction, error) {
		varsAsMapAny := make(map[string]any)
		for k, v := range vars {
			varsAsMapAny[k] = v
		}
		response, err := r.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
			V2PostTransaction: shared.V2PostTransaction{
				Script: &shared.V2PostTransactionScript{
					Plain: script,
					Vars:  varsAsMapAny,
				},
			},
			Ledger: r.ledger.Name,
		})
		if err != nil {
			return nil, errors.Wrap(err, "creating transaction")
		}

		return &ledger.Transaction{
			TransactionData: ledger.TransactionData{
				Postings: collectionutils.Map(response.V2CreateTransactionResponse.Data.Postings, func(from shared.V2Posting) ledger.Posting {
					return ledger.Posting{
						Source:      from.Source,
						Destination: from.Destination,
						Amount:      from.Amount,
						Asset:       from.Asset,
					}
				}),
				Metadata: response.V2CreateTransactionResponse.Data.Metadata,
				Timestamp: time.Time{
					Time: response.V2CreateTransactionResponse.Data.Timestamp,
				},
				Reference: func() string {
					if response.V2CreateTransactionResponse.Data.Reference == nil {
						return ""
					}
					return *response.V2CreateTransactionResponse.Data.Reference
				}(),
			},
			ID:       int(response.V2CreateTransactionResponse.Data.ID.Int64()),
			Reverted: response.V2CreateTransactionResponse.Data.Reverted,
		}, nil
	})
}

func (r *RemoteStackEnv) Stop() error {
	return nil
}

func NewRemoveStackEnv(client *formance.Formance, ledger ledger.Ledger) *RemoteStackEnv {
	return &RemoteStackEnv{
		client: client,
		ledger: ledger,
	}
}

var _ Env = (*RemoteStackEnv)(nil)
