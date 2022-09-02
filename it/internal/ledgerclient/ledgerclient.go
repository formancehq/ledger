package ledgerclient

import (
	"net/http"

	"github.com/numary/ledger/it/internal/openapi3"
	ledgerclient "github.com/numary/numary-sdk-go"
)

var globalClient *ledgerclient.APIClient

func Set(client *ledgerclient.APIClient) {
	globalClient = client
}

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
