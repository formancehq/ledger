package server

import (
	"context"
	"net/http"

	ledgerclient "github.com/numary/ledger/tests/internal/client"
	. "github.com/onsi/gomega"
)

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

func (d defaultClient) RevertTransaction(id int32) ledgerclient.ApiRevertTransactionRequest {
	return d.underlying.TransactionsApi.RevertTransaction(context.Background(), CurrentLedger(), id)
}

func GetClient() *defaultClient {
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

func RevertTransaction(id int32) ledgerclient.ApiRevertTransactionRequest {
	return GetClient().RevertTransaction(id)
}

func MustExecute[T any](request interface {
	Execute() (value T, httpResponse *http.Response, err error)
}) (T, *http.Response) {
	ret, httpResponse, err := request.Execute()
	Expect(err).To(BeNil())
	return ret, httpResponse
}
