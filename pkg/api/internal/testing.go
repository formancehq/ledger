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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedauth"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/routes"
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

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"scope": strings.Join(routes.AllScopes, " "),
	})
	signed, err := token.SignedString([]byte("0000000000000000"))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", signed))
	return req, rec
}

func PostTransaction(t *testing.T, handler http.Handler, tx core.TransactionData) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/transactions", testingLedger), Buffer(t, tx))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionBatch(t *testing.T, handler http.Handler, txs core.Transactions) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, "/"+testingLedger+"/transactions/batch", Buffer(t, txs))
	handler.ServeHTTP(rec, req)
	return rec
}

func PostTransactionPreview(t *testing.T, handler http.Handler, tx core.TransactionData) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/transactions?preview=true", testingLedger), Buffer(t, tx))
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

func GetStats(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/stats", testingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func LoadMapping(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/%s/mapping", testingLedger), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func SaveMapping(t *testing.T, handler http.Handler, m core.Mapping) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPut, fmt.Sprintf("/%s/mapping", testingLedger), Buffer(t, m))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetInfo(handler http.Handler) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, "/_info", nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func PostScript(t *testing.T, handler http.Handler, s core.Script, query url.Values) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, fmt.Sprintf("/%s/script", testingLedger), Buffer(t, s))
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
		api.Module(api.Config{StorageDriver: "sqlite", Version: "latest", UseScopes: true}),
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
					}(store, ctx)

					_, err = store.Initialize(ctx)
					return err
				},
			})
		}),
		fx.NopLogger,
	}, options...)

	options = append(options, routes.ProvidePerLedgerMiddleware(func() []gin.HandlerFunc {
		return []gin.HandlerFunc{
			func(c *gin.Context) {
				handled := false
				sharedauth.Middleware(sharedauth.NewHttpBearerMethod(
					sharedauth.NoOpValidator,
				))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handled = true
					// The middleware replace the context of the request to include the agent
					// We have to forward it to gin
					c.Request = r
					c.Next()
				})).ServeHTTP(c.Writer, c.Request)
				if !handled {
					c.Abort()
				}
			},
		}
	}, fx.ParamTags(`optional:"true"`)))

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
