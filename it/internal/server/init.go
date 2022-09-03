package server

import (
	"net/http"
	"testing"

	ledgerclient "github.com/numary/ledger/it/internal/client"
	"github.com/numary/ledger/it/internal/openapi3"
)

var globalClient *ledgerclient.APIClient

func Init(ledgerUrl string) {
	cp := *http.DefaultClient
	httpClient := &cp
	httpClient.Transport = openapi3.NewTransport(ledgerUrl)

	clientConfiguration := ledgerclient.NewConfiguration()
	clientConfiguration.HTTPClient = httpClient
	clientConfiguration.Servers[0].URL = ledgerUrl
	clientConfiguration.Debug = testing.Verbose()
	globalClient = ledgerclient.NewAPIClient(clientConfiguration)
}
