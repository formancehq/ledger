package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	ledgerclient "github.com/numary/ledger/it/internal/client"
	"github.com/numary/ledger/it/internal/openapi3"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/pkg/errors"
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

type Client interface {
	CreateTransaction() ledgerclient.ApiCreateTransactionRequest
	ListTransactions() ledgerclient.ApiListTransactionsRequest
	GetInfo() ledgerclient.ApiGetInfoRequest
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

type isLedgerErrorCode struct {
	code ledgerclient.ErrorCode
}

func (a isLedgerErrorCode) Match(actual interface{}) (success bool, err error) {
	err, ok := actual.(error)
	if !ok {
		return false, errors.New("have trace expect an object of type error")
	}

	ledgerErr, ok := err.(ledgerclient.GenericOpenAPIError)
	if !ok {
		return false, errors.New("error is not of type ledgerclient.GenericOpenAPIError")
	}

	response := &ledgerclient.ErrorResponse{}
	if err := json.Unmarshal(ledgerErr.Body(), response); err != nil {
		return false, err
	}

	if response.ErrorCode != a.code {
		return false, errors.New("error is not of type ledgerclient.GenericOpenAPIError")
	}

	return true, nil
}

func (a isLedgerErrorCode) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to have code \r\n%#v", actual, a.code)
}

func (a isLedgerErrorCode) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("expected '%s' to not have code \r\n%#v", actual, a.code)
}

var _ types.GomegaMatcher = &isLedgerErrorCode{}

func HaveLedgerErrorCode(code ledgerclient.ErrorCode) *isLedgerErrorCode {
	return &isLedgerErrorCode{
		code: code,
	}
}
