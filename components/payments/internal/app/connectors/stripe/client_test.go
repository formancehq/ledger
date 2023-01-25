package stripe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v72"
)

type httpMockExpectation interface {
	handle(t *testing.T, r *http.Request) (*http.Response, error)
}

type httpMock struct {
	t            *testing.T
	expectations []httpMockExpectation
	mu           sync.Mutex
}

func (mock *httpMock) RoundTrip(request *http.Request) (*http.Response, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.expectations) == 0 {
		return nil, fmt.Errorf("no more expectations")
	}

	expectations := mock.expectations[0]
	if len(mock.expectations) == 1 {
		mock.expectations = make([]httpMockExpectation, 0)
	} else {
		mock.expectations = mock.expectations[1:]
	}

	return expectations.handle(mock.t, request)
}

var _ http.RoundTripper = &httpMock{}

type HTTPExpect[REQUEST any, RESPONSE any] struct {
	statusCode   int
	path         string
	method       string
	requestBody  *REQUEST
	responseBody *RESPONSE
	queryParams  map[string]any
}

func (e *HTTPExpect[REQUEST, RESPONSE]) handle(t *testing.T, request *http.Request) (*http.Response, error) {
	t.Helper()

	if e.path != request.URL.Path {
		return nil, fmt.Errorf("expected url was '%s', got, '%s'", e.path, request.URL.Path)
	}

	if e.method != request.Method {
		return nil, fmt.Errorf("expected method was '%s', got, '%s'", e.method, request.Method)
	}

	if e.requestBody != nil {
		body := new(REQUEST)

		err := json.NewDecoder(request.Body).Decode(body)
		if err != nil {
			panic(err)
		}

		if !reflect.DeepEqual(*e.responseBody, *body) {
			return nil, fmt.Errorf("mismatch body")
		}
	}

	for key, value := range e.queryParams {
		qpvalue := ""

		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			qpvalue = fmt.Sprintf("%d", value)
		default:
			qpvalue = fmt.Sprintf("%s", value)
		}

		if rvalue := request.URL.Query().Get(key); rvalue != qpvalue {
			return nil, fmt.Errorf("expected query param '%s' with value '%s', got '%s'", key, qpvalue, rvalue)
		}
	}

	data := make([]byte, 0)

	if e.responseBody != nil {
		var err error

		data, err = json.Marshal(e.responseBody)
		if err != nil {
			panic(err)
		}
	}

	return &http.Response{
		StatusCode:    e.statusCode,
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: int64(len(data)),
		Request:       request,
	}, nil
}

func (e *HTTPExpect[REQUEST, RESPONSE]) Path(p string) *HTTPExpect[REQUEST, RESPONSE] {
	e.path = p

	return e
}

func (e *HTTPExpect[REQUEST, RESPONSE]) Method(p string) *HTTPExpect[REQUEST, RESPONSE] {
	e.method = p

	return e
}

func (e *HTTPExpect[REQUEST, RESPONSE]) Body(body *REQUEST) *HTTPExpect[REQUEST, RESPONSE] {
	e.requestBody = body

	return e
}

func (e *HTTPExpect[REQUEST, RESPONSE]) QueryParam(key string, value any) *HTTPExpect[REQUEST, RESPONSE] {
	e.queryParams[key] = value

	return e
}

func (e *HTTPExpect[REQUEST, RESPONSE]) RespondsWith(statusCode int,
	body *RESPONSE,
) *HTTPExpect[REQUEST, RESPONSE] {
	e.statusCode = statusCode
	e.responseBody = body

	return e
}

func Expect[REQUEST, RESPONSE any](mock *httpMock) *HTTPExpect[REQUEST, RESPONSE] {
	expectations := &HTTPExpect[REQUEST, RESPONSE]{
		queryParams: map[string]any{},
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	mock.expectations = append(mock.expectations, expectations)

	return expectations
}

type StripeBalanceTransactionListExpect struct {
	*HTTPExpect[struct{}, MockedListResponse]
}

func (e *StripeBalanceTransactionListExpect) Path(p string) *StripeBalanceTransactionListExpect {
	e.HTTPExpect.Path(p)

	return e
}

func (e *StripeBalanceTransactionListExpect) Method(p string) *StripeBalanceTransactionListExpect {
	e.HTTPExpect.Method(p)

	return e
}

func (e *StripeBalanceTransactionListExpect) QueryParam(key string,
	value any,
) *StripeBalanceTransactionListExpect {
	e.HTTPExpect.QueryParam(key, value)

	return e
}

func (e *StripeBalanceTransactionListExpect) RespondsWith(statusCode int, hasMore bool,
	body ...*stripe.BalanceTransaction,
) *StripeBalanceTransactionListExpect {
	e.HTTPExpect.RespondsWith(statusCode, &MockedListResponse{
		HasMore: hasMore,
		Data:    body,
	})

	return e
}

func (e *StripeBalanceTransactionListExpect) StartingAfter(v string) *StripeBalanceTransactionListExpect {
	e.QueryParam("starting_after", v)

	return e
}

func (e *StripeBalanceTransactionListExpect) CreatedLte(v time.Time) *StripeBalanceTransactionListExpect {
	e.QueryParam("created[lte]", v.Unix())

	return e
}

func (e *StripeBalanceTransactionListExpect) Limit(v int) *StripeBalanceTransactionListExpect {
	e.QueryParam("limit", v)

	return e
}

func ExpectBalanceTransactionList(mock *httpMock) *StripeBalanceTransactionListExpect {
	e := Expect[struct{}, MockedListResponse](mock)
	e.Path("/v1/balance_transactions").Method(http.MethodGet)

	return &StripeBalanceTransactionListExpect{
		HTTPExpect: e,
	}
}

func DatePtr(t time.Time) *time.Time {
	return &t
}

type BalanceTransactionSource stripe.BalanceTransactionSource

func (t *BalanceTransactionSource) MarshalJSON() ([]byte, error) {
	type Aux BalanceTransactionSource

	return json.Marshal(struct {
		Aux
		Charge   *stripe.Charge   `json:"charge"`
		Payout   *stripe.Payout   `json:"payout"`
		Refund   *stripe.Refund   `json:"refund"`
		Transfer *stripe.Transfer `json:"transfer"`
	}{
		Aux:      Aux(*t),
		Charge:   t.Charge,
		Payout:   t.Payout,
		Refund:   t.Refund,
		Transfer: t.Transfer,
	})
}

type BalanceTransaction stripe.BalanceTransaction

func (t *BalanceTransaction) MarshalJSON() ([]byte, error) {
	type Aux BalanceTransaction

	return json.Marshal(struct {
		Aux
		Source *BalanceTransactionSource `json:"source"`
	}{
		Aux:    Aux(*t),
		Source: (*BalanceTransactionSource)(t.Source),
	})
}

//nolint:tagliatelle // allow snake_case in client
type MockedListResponse struct {
	HasMore bool                         `json:"has_more"`
	Data    []*stripe.BalanceTransaction `json:"data"`
}

func (t *MockedListResponse) MarshalJSON() ([]byte, error) {
	type Aux MockedListResponse

	txs := make([]*BalanceTransaction, 0)
	for _, tx := range t.Data {
		txs = append(txs, (*BalanceTransaction)(tx))
	}

	return json.Marshal(struct {
		Aux
		Data []*BalanceTransaction `json:"data"`
	}{
		Aux:  Aux(*t),
		Data: txs,
	})
}
