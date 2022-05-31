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
	"reflect"
	"testing"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
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

func DecodeSingleResponse(t *testing.T, reader io.Reader, v interface{}) bool {
	type Response struct {
		Data json.RawMessage `json:"data"`
	}

	res := Response{}
	if !Decode(t, reader, &res) {
		return false
	}

	if !Decode(t, bytes.NewBuffer(res.Data), v) {
		return false
	}

	return true
}

func DecodeCursorResponse(t *testing.T, reader io.Reader, targetType interface{}) *sharedapi.Cursor {
	type Response struct {
		Cursor json.RawMessage `json:"cursor"`
	}
	res := Response{}
	Decode(t, reader, &res)

	type Cursor struct {
		sharedapi.Cursor
		Data []json.RawMessage `json:"data"`
	}
	cursor := Cursor{}
	Decode(t, bytes.NewBuffer(res.Cursor), &cursor)

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

func PostTransactionMetadata(t *testing.T, handler http.Handler, id uint64, m core.Metadata) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/"+testingLedger+"/transactions/%d/metadata", id), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func CountTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, "/"+testingLedger+"/transactions", nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransactions(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/transactions", nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransaction(handler http.Handler, id uint64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/"+testingLedger+"/transactions/%d", id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func CountAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodHead, "/"+testingLedger+"/accounts", nil)
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetAccounts(handler http.Handler, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/"+testingLedger+"/accounts", nil)
	req.URL.RawQuery = query.Encode()
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

func PostScript(t *testing.T, handler http.Handler, s core.Script, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, "/"+testingLedger+"/script", Buffer(t, s))
	req.URL.RawQuery = query.Encode()
	handler.ServeHTTP(rec, req)
	return rec
}

func GetStore(t *testing.T, driver storage.Driver, ctx context.Context) storage.Store {
	store, _, err := driver.GetStore(ctx, testingLedger, true)
	require.NoError(t, err)
	return store
}

func RunTest(t *testing.T, options ...fx.Option) {
	l := logrus.New()
	if testing.Verbose() {
		l.Level = logrus.DebugLevel
	}
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	testingLedger = uuid.New()
	ch := make(chan struct{})

	options = append([]fx.Option{
		api.Module(api.Config{StorageDriver: "sqlite", Version: "latest"}),
		ledger.ResolveModule(),
		ledgertesting.ProvideStorageDriver(),
		fx.Invoke(func(driver storage.Driver, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					store, _, err := driver.GetStore(ctx, testingLedger, true)
					if err != nil {
						return err
					}
					defer func(store storage.Store, ctx context.Context) {
						require.NoError(t, store.Close(ctx))
					}(store, context.Background())

					_, err = store.Initialize(context.Background())
					return err
				},
			})
		}),
		fx.NopLogger,
	}, options...)

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

func RunSubTest(t *testing.T, name string, opts ...fx.Option) {
	t.Run(name, func(t *testing.T) {
		RunTest(t, opts...)
	})
}
