//go:build it

package env

import (
	"context"
	"net/http"
	"testing"

	ledgerclient "github.com/formancehq/ledger/pkg/client"
)

type RemoteLedgerEnvFactory struct {
	httpClient *http.Client
	ledgerURL  string
}

func (r *RemoteLedgerEnvFactory) Create(_ context.Context, _ *testing.B) Env {
	return NewRemoteLedgerEnv(ledgerclient.New(
		ledgerclient.WithClient(r.httpClient),
		ledgerclient.WithServerURL(r.ledgerURL),
	))
}

var _ EnvFactory = (*RemoteLedgerEnvFactory)(nil)

func NewRemoteLedgerEnvFactory(httpClient *http.Client, ledgerURL string) *RemoteLedgerEnvFactory {
	return &RemoteLedgerEnvFactory{
		httpClient: httpClient,
		ledgerURL:  ledgerURL,
	}
}

type RemoteLedgerEnv struct {
	client    *ledgerclient.Formance
}

func (r *RemoteLedgerEnv) Client() *ledgerclient.Formance {
	return r.client
}

func (r *RemoteLedgerEnv) Stop(_ context.Context) error {
	return nil
}

func NewRemoteLedgerEnv(client *ledgerclient.Formance) *RemoteLedgerEnv {
	return &RemoteLedgerEnv{
		client:    client,
	}
}

var _ Env = (*RemoteLedgerEnv)(nil)
