//go:build it

package env

import (
	"context"
	"net/http"
	"testing"

	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/stretchr/testify/require"
)

type RemoteLedgerEnvFactory struct {
	httpClient *http.Client
	ledgerURL  string
}

func (r *RemoteLedgerEnvFactory) Create(ctx context.Context, b *testing.B) Env {
	client := ledgerclient.New(
		ledgerclient.WithClient(r.httpClient),
		ledgerclient.WithServerURL(r.ledgerURL),
	)

	_, err := client.Ledger.V2.CreateLedger(ctx, ledger.Name, &components.V2CreateLedgerRequest{
		Bucket:   &ledger.Bucket,
		Metadata: ledger.Metadata,
		Features: ledger.Features,
	})
	require.NoError(b, err)

	return NewRemoteLedgerEnv(client, ledgerURLFlag, ledger)
}

var _ EnvFactory = (*RemoteLedgerEnvFactory)(nil)

func NewRemoteLedgerEnvFactory(httpClient *http.Client, ledgerURL string) *RemoteLedgerEnvFactory {
	return &RemoteLedgerEnvFactory{
		httpClient: httpClient,
		ledgerURL:  ledgerURL,
	}
}

type RemoteLedgerEnv struct {
	url        string
	client     *ledgerclient.SDK
	metricsURL string
	ledger     ledger.Ledger
}

func (r *RemoteLedgerEnv) URL() string {
	return r.url
}

func (r *RemoteLedgerEnv) Client() *ledgerclient.SDK {
	return r.client
}

func (r *RemoteLedgerEnv) Stop(_ context.Context) error {
	return nil
}

func NewRemoteLedgerEnv(client *ledgerclient.SDK, metricsURL string, ledger ledger.Ledger) *RemoteLedgerEnv {
	return &RemoteLedgerEnv{
		client:     client,
		metricsURL: metricsURL,
		ledger:     ledger,
		url:        metricsURL,
	}
}

var _ Env = (*RemoteLedgerEnv)(nil)
