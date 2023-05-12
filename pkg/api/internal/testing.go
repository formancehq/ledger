package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

var testingLedger string

func Encode(t *testing.T, v interface{}) []byte {
	data, err := json.Marshal(v)
	assert.NoError(t, err)
	return data
}

func Buffer(t *testing.T, v interface{}) *bytes.Buffer {
	return bytes.NewBuffer(Encode(t, v))
}

func Decode(t *testing.T, reader io.Reader, v interface{}) bool {
	err := json.NewDecoder(reader).Decode(v)
	return assert.NoError(t, err)
}

func DecodeSingleResponse[T any](t *testing.T, reader io.Reader) (T, bool) {
	res := sharedapi.BaseResponse[T]{}
	if !Decode(t, reader, &res) {
		var zero T
		return zero, false
	}
	return *res.Data, true
}

func DecodeCursorResponse[T any](t *testing.T, reader io.Reader) *sharedapi.Cursor[T] {
	res := sharedapi.BaseResponse[T]{}
	Decode(t, reader, &res)
	return res.Cursor
}

func NewRequest(method, path string, body io.Reader) (*http.Request, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")

	return req, rec
}

func PostTransaction(t *testing.T, handler http.Handler, payload controllers.PostTransaction, preview bool) *httptest.ResponseRecorder {
	path := fmt.Sprintf("/%s/transactions", testingLedger)
	if preview {
		path += "?preview=true"
	}
	req, rec := NewRequest(http.MethodPost, path, Buffer(t, payload))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionMetadata(t *testing.T, handler http.Handler, id uint64, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/transactions/%d/metadata", testingLedger, id), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func CountTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, fmt.Sprintf("/%s/transactions", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/transactions", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransaction(handler http.Handler, id uint64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/transactions/%d", testingLedger, id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func RevertTransaction(handler http.Handler, id uint64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/"+testingLedger+"/transactions/%d/revert", id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func CountAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, fmt.Sprintf("/%s/accounts", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/accounts", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetBalances(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/balances", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetBalancesAggregated(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/aggregate/balances", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccount(handler http.Handler, addr string) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/accounts/%s", testingLedger, addr), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func PostAccountMetadata(t *testing.T, handler http.Handler, addr string, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/accounts/%s/metadata", testingLedger, addr), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func NewRequestOnLedger(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s%s", testingLedger, path), Buffer(t, body))
	handler.ServeHTTP(rec, req)
	return rec
}

func NewGetOnLedger(handler http.Handler, path string) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s%s", testingLedger, path), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func NewPostOnLedger(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s%s", testingLedger, path), Buffer(t, body))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/_info", testingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerStats(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/stats", testingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerLogs(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/logs", testingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/_info", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func PostScript(t *testing.T, handler http.Handler, s core.ScriptData, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/script", testingLedger), Buffer(t, s))
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerStore(t *testing.T, driver storage.Driver[ledger.Store], ctx context.Context) ledger.Store {
	store, _, err := driver.GetLedgerStore(ctx, testingLedger, true)
	require.NoError(t, err)
	return store
}

func RunTest(t *testing.T, options ...fx.Option) {
	testingLedger = uuid.New()
	ch := make(chan struct{})

	options = append([]fx.Option{
		api.Module(api.Config{StorageDriver: "sqlite", Version: "latest"}),
		// 100 000 000 bytes is 100 MB
		ledger.ResolveModule(100000000, 100),
		ledgertesting.ProvideLedgerStorageDriver(),
		fx.Invoke(func(driver storage.Driver[ledger.Store], lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					store, _, err := driver.GetLedgerStore(ctx, testingLedger, true)
					if err != nil {
						return err
					}
					defer func(store ledger.Store, ctx context.Context) {
						require.NoError(t, store.Close(ctx))
					}(store, ctx)

					_, err = store.Initialize(ctx)
					return err
				},
			})
		}),
		fx.NopLogger,
	}, options...)

	options = append(options, fx.Provide(
		fx.Annotate(func() []ledger.LedgerOption {
			ledgerOptions := []ledger.LedgerOption{}

			return ledgerOptions
		}, fx.ResultTags(ledger.ResolverLedgerOptionsKey)),
	))

	options = append(options,
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					close(ch)
					return nil
				},
			})
		}))

	app := fx.New(options...)

	assert.NoError(t, app.Start(context.Background()))

	select {
	case <-ch:
	default:
		if app.Err() != nil {
			assert.Fail(t, app.Err().Error())
		}
	}
}
