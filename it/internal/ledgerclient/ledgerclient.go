package ledgerclient

import (
	"net/http"

	ledgerclient "github.com/numary/ledger/it/internal/client"
	"github.com/numary/ledger/it/internal/openapi3"
)

var globalClient *ledgerclient.APIClient

func Set(client *ledgerclient.APIClient) {
	globalClient = client
}

type APIClient = ledgerclient.APIClient

func Client() *ledgerclient.APIClient {
	return globalClient
}

func Init(ledgerUrl string) {
	cp := *http.DefaultClient
	httpClient := &cp
	httpClient.Transport = openapi3.NewTransport(ledgerUrl)

	clientConfiguration := ledgerclient.NewConfiguration()
	clientConfiguration.HTTPClient = httpClient
	clientConfiguration.Servers[0].URL = ledgerUrl
	globalClient = ledgerclient.NewAPIClient(clientConfiguration)
}
