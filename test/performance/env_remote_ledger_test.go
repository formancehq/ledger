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

type RemoteLedgerEnvFactory struct {
	httpClient *http.Client
	ledgerURL  string
}

func (r *RemoteLedgerEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

	// todo: use standalone sdk only
	client := ledgerclient.New(
		ledgerclient.WithClient(r.httpClient),
		ledgerclient.WithServerURL(r.ledgerURL),
	)

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: ledger.Name,
		V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
			Bucket:   &ledger.Bucket,
			Metadata: ledger.Metadata,
			Features: ledger.Features,
		},
	})
	require.NoError(b, err)

	return NewRemoteLedgerEnv(client, ledger)
}

var _ EnvFactory = (*RemoteLedgerEnvFactory)(nil)

func NewRemoteLedgerEnvFactory(httpClient *http.Client, ledgerURL string) *RemoteLedgerEnvFactory {
	return &RemoteLedgerEnvFactory{
		httpClient: httpClient,
		ledgerURL:  ledgerURL,
	}
}

type RemoteLedgerEnv struct {
	ledger ledger.Ledger
	client *ledgerclient.Formance
}

func (r *RemoteLedgerEnv) Executor() TransactionExecutor {
	return TransactionExecutorFn(func(ctx context.Context, script string, vars map[string]string) (*ledger.Transaction, error) {
		varsAsMapAny := make(map[string]any)
		for k, v := range vars {
			varsAsMapAny[k] = v
		}
		response, err := r.client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
			Ledger: r.ledger.Name,
			V2PostTransaction: components.V2PostTransaction{
				Script: &components.V2PostTransactionScript{
					Plain: script,
					Vars:  varsAsMapAny,
				},
			},
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
			ID:       int(response.V2CreateTransactionResponse.Data.ID.Int64()),
			Reverted: response.V2CreateTransactionResponse.Data.Reverted,
		}, nil
	})
}

func (r *RemoteLedgerEnv) Stop() error {
	return nil
}

func NewRemoteLedgerEnv(client *ledgerclient.Formance, ledger ledger.Ledger) *RemoteLedgerEnv {
	return &RemoteLedgerEnv{
		client: client,
		ledger: ledger,
	}
}

var _ Env = (*RemoteLedgerEnv)(nil)
