//go:build it

package performance_test

import (
	"context"
	"net/http"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	ledgerclient "github.com/formancehq/stack/ledger/client"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/stretchr/testify/require"
)

type RemoteLedgerEnvFactory struct {
	httpClient *http.Client
	ledgerURL  string
}

func (r *RemoteLedgerEnvFactory) Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env {

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

	return NewRemoteLedgerEnv(client, ledgerURL, ledger)
}

var _ EnvFactory = (*RemoteLedgerEnvFactory)(nil)

func NewRemoteLedgerEnvFactory(httpClient *http.Client, ledgerURL string) *RemoteLedgerEnvFactory {
	return &RemoteLedgerEnvFactory{
		httpClient: httpClient,
		ledgerURL:  ledgerURL,
	}
}

type RemoteLedgerEnv struct {
	ledger    ledger.Ledger
	client    *ledgerclient.Formance
	ledgerURL string
}

func (r *RemoteLedgerEnv) URL() string {
	return r.ledgerURL
}

func (r *RemoteLedgerEnv) Client() *ledgerclient.Formance {
	return r.client
}

func (r *RemoteLedgerEnv) Stop(_ context.Context) error {
	return nil
}

func NewRemoteLedgerEnv(client *ledgerclient.Formance, metricsURL string, ledger ledger.Ledger) *RemoteLedgerEnv {
	return &RemoteLedgerEnv{
		client:    client,
		ledger:    ledger,
		ledgerURL: metricsURL,
	}
}

var _ Env = (*RemoteLedgerEnv)(nil)
