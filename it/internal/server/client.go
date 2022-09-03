package server

import (
	"context"
	"net/http"

	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/onsi/gomega"
)

type Client interface {
	CreateTransaction() ledgerclient.ApiCreateTransactionRequest
	ListTransactions() ledgerclient.ApiListTransactionsRequest
	GetInfo() ledgerclient.ApiGetInfoRequest
	GetSwaggerAsJSON() (*http.Response, error)
	GetSwaggerAsYAML() (*http.Response, error)
}

type defaultClient struct {
	underlying *ledgerclient.APIClient
}

func (d defaultClient) ListTransactions() ledgerclient.ApiListTransactionsRequest {
	return d.underlying.TransactionsApi.ListTransactions(context.Background(), CurrentLedger())
}

func (d defaultClient) GetInfo() ledgerclient.ApiGetInfoRequest {
	return d.underlying.ServerApi.GetInfo(context.Background())
}

func (d defaultClient) CreateTransaction() ledgerclient.ApiCreateTransactionRequest {
	return d.underlying.TransactionsApi.CreateTransaction(context.Background(), CurrentLedger())
}

func (d defaultClient) GetSwaggerAsJSON() (*http.Response, error) {
	return http.DefaultClient.Get(d.underlying.GetConfig().Servers[0].URL + "/swagger.json")
}

func (d defaultClient) GetSwaggerAsYAML() (*http.Response, error) {
	return http.DefaultClient.Get(d.underlying.GetConfig().Servers[0].URL + "/swagger.yaml")
}

var _ Client = &defaultClient{}

func GetClient() Client {
	return &defaultClient{
		underlying: globalClient,
	}
}

func ListTransactions() ledgerclient.ApiListTransactionsRequest {
	return GetClient().ListTransactions()
}

func GetInfo() ledgerclient.ApiGetInfoRequest {
	return GetClient().GetInfo()
}

func GetSwaggerAsJSON() (*http.Response, error) {
	return GetClient().GetSwaggerAsJSON()
}

func GetSwaggerAsYAML() (*http.Response, error) {
	return GetClient().GetSwaggerAsYAML()
}

func CreateTransaction() ledgerclient.ApiCreateTransactionRequest {
	return GetClient().CreateTransaction()
}

func MustExecute[T any](request interface {
	Execute() (value T, httpResponse *http.Response, err error)
}) (T, *http.Response) {
	ret, httpResponse, err := request.Execute()
	Expect(err).To(BeNil())
	return ret, httpResponse
}
