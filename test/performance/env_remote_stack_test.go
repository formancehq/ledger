//go:build it

package performance_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgerclient "github.com/formancehq/stack/ledger/client"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type RemoteStackEnvFactory struct {
	httpClient *http.Client
	stackURL   string
}

func (r *RemoteStackEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

	client := ledgerclient.New(
		ledgerclient.WithClient(r.httpClient),
		ledgerclient.WithServerURL(r.stackURL+"/api/ledger"),
	)

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
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
	client *ledgerclient.Formance
}

func (r *RemoteStackEnv) Executor() TransactionExecutor {
	return TransactionExecutorFn(func(ctx context.Context, script string, vars map[string]string) (*ledger.Transaction, error) {
		varsAsMapAny := make(map[string]any)
		for k, v := range vars {
			varsAsMapAny[k] = v
		}
		response, err := r.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
			V2PostTransaction: components.V2PostTransaction{
				Script: &components.V2PostTransactionScript{
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
				Postings: collectionutils.Map(response.V2CreateTransactionResponse.Data.Postings, func(from components.V2Posting) ledger.Posting {
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
			ID: int(response.V2CreateTransactionResponse.Data.ID.Int64()),
			RevertedAt: func() *time.Time {
				if response.V2CreateTransactionResponse.Data.RevertedAt == nil {
					return nil
				}
				return &time.Time{Time: *response.V2CreateTransactionResponse.Data.RevertedAt}
			}(),
		}, nil
	})
}

func (r *RemoteStackEnv) Stop() error {
	return nil
}

func NewRemoveStackEnv(client *ledgerclient.Formance, ledger ledger.Ledger) *RemoteStackEnv {
	return &RemoteStackEnv{
		client: client,
		ledger: ledger,
	}
}

var _ Env = (*RemoteStackEnv)(nil)
