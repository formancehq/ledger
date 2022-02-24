package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
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

func Decode(t *testing.T, reader io.Reader, v interface{}) {
	err := json.NewDecoder(reader).Decode(v)
	assert.NoError(t, err)
}

func DecodeSingleResponse(t *testing.T, reader io.Reader, v interface{}) {
	type Response struct {
		Data json.RawMessage `json:"data"`
	}
	res := Response{}
	Decode(t, reader, &res)
	Decode(t, bytes.NewBuffer(res.Data), v)
}

func DecodeCursorResponse(t *testing.T, reader io.Reader, targetType interface{}) *sharedapi.Cursor {
	type Cursor struct {
		sharedapi.Cursor
		Data []json.RawMessage `json:"data"`
	}
	type Response struct {
		Cursor json.RawMessage `json:"cursor"`
	}
	res := Response{}
	Decode(t, reader, &res)

	cursor := &Cursor{}
	Decode(t, bytes.NewBuffer(res.Cursor), cursor)

	items := make([]interface{}, 0)
	for _, d := range cursor.Data {
		target := reflect.New(reflect.TypeOf(targetType)).Interface()
		Decode(t, bytes.NewBuffer(d), target)
		items = append(items, reflect.ValueOf(target).Elem().Interface())
	}
	cursor.Cursor.Data = items

	return &cursor.Cursor
}

func NewRequest(method, path string, body io.Reader) (*http.Request, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	return req, rec
}

func PostTransaction(t *testing.T, handler http.Handler, tx core.TransactionData) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, "/"+testingLedger+"/transactions", Buffer(t, tx))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionPreview(t *testing.T, handler http.Handler, tx core.TransactionData) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, "/"+testingLedger+"/transactions?preview=true", Buffer(t, tx))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionMetadata(t *testing.T, handler http.Handler, id int64, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/"+testingLedger+"/transactions/%d/metadata", id), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransactions(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/transactions", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransaction(handler http.Handler, id int64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/"+testingLedger+"/transactions/%d", id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccounts(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/accounts", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccount(handler http.Handler, addr string) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/accounts/"+addr, nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func PostAccountMetadata(t *testing.T, handler http.Handler, addr string, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/"+testingLedger+"/accounts/%s/metadata", addr), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetStats(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/stats", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func LoadMapping(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/mapping", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func SaveMapping(t *testing.T, handler http.Handler, m core.Mapping) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPut, "/"+testingLedger+"/mapping", Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/_info", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func WithNewModule(t *testing.T, options ...fx.Option) {
	testingLedger = uuid.New()
	module := api.Module(api.Config{
		StorageDriver: "sqlite",
		LedgerLister: controllers.LedgerListerFn(func(r *http.Request) []string {
			return []string{
				"quickstart",
			}
		}),
		Version: "latest",
	})
	ch := make(chan struct{})
	options = append([]fx.Option{
		module,
		ledger.ResolveModule(),
		storage.DefaultModule(),
		ledgertesting.StorageModule(),
		logging.LogrusModule(),
		fx.NopLogger,
	}, options...)
	options = append(options, fx.Invoke(func() {
		close(ch)
	}))

	app := fx.New(options...)

	select {
	case <-ch:
	default:
		assert.Fail(t, app.Err().Error())
	}
}

func RunSubTest(t *testing.T, name string, fn interface{}) {
	t.Run(name, func(t *testing.T) {
		RunTest(t, fn)
	})
}

func RunTest(t *testing.T, fn interface{}) {
	WithNewModule(t, fx.Invoke(fn))
}
