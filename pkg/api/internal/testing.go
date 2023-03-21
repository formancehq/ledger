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

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var TestingLedger string

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

func PostTransaction(t *testing.T, handler http.Handler, payload controllers.PostTransactionRequest, preview bool) *httptest.ResponseRecorder {
	path := fmt.Sprintf("/%s/transactions", TestingLedger)
	if preview {
		path += "?preview=true"
	}
	req, rec := NewRequest(http.MethodPost, path, Buffer(t, payload))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionMetadata(t *testing.T, handler http.Handler, id uint64, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/transactions/%d/metadata", TestingLedger, id), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func CountTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, fmt.Sprintf("/%s/transactions", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/transactions", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransaction(handler http.Handler, id uint64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/transactions/%d", TestingLedger, id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func RevertTransaction(handler http.Handler, id uint64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/"+TestingLedger+"/transactions/%d/revert", id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func CountAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, fmt.Sprintf("/%s/accounts", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/accounts", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetBalances(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/balances", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetBalancesAggregated(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/aggregate/balances", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccount(handler http.Handler, addr string) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/accounts/%s", TestingLedger, addr), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func PostAccountMetadata(t *testing.T, handler http.Handler, addr string, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/accounts/%s/metadata", TestingLedger, addr), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func NewRequestOnLedger(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s%s", TestingLedger, path), Buffer(t, body))
	handler.ServeHTTP(rec, req)
	return rec
}

func NewGetOnLedger(handler http.Handler, path string) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s%s", TestingLedger, path), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func NewPostOnLedger(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s%s", TestingLedger, path), Buffer(t, body))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/_info", TestingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerStats(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/stats", TestingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerLogs(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/logs", TestingLedger), nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/_info", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetLedgerStore(t *testing.T, driver storage.Driver, ctx context.Context) storage.LedgerStore {
	store, _, err := driver.GetLedgerStore(ctx, TestingLedger, true)
	require.NoError(t, err)
	return store
}

func RunTest(t *testing.T, callback func(api chi.Router, storageDriver storage.Driver)) {
	TestingLedger = uuid.New()
	t.Parallel()

	storageDriver := ledgertesting.StorageDriver(t)
	require.NoError(t, storageDriver.Initialize(context.Background()))

	cacheManager := cache.NewManager(storageDriver)
	lock := lock.NewInMemory()
	runnerManager := runner.NewManager(storageDriver, lock, cacheManager, false)
	queryWorker := query.NewWorker(query.DefaultWorkerConfig, storageDriver, query.NewNoOpMonitor())
	go func() {
		require.NoError(t, queryWorker.Run(context.Background()))
	}()
	resolver := ledger.NewResolver(storageDriver, lock, cacheManager, runnerManager, queryWorker)

	router := routes.NewRouter(storageDriver, "latest", resolver,
		logging.FromContext(context.Background()), &health.HealthController{})

	callback(router, storageDriver)
}
