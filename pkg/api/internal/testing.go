package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Encode(t *testing.T, v interface{}) []byte {
	data, err := json.Marshal(v)
	assert.NoError(t, err)
	return data
}

func Buffer(t *testing.T, v interface{}) *bytes.Buffer {
	return bytes.NewBuffer(Encode(t, v))
}

func DecodeResponse(t *testing.T, reader io.Reader, v interface{}) {
	type Response struct {
		Data json.RawMessage `json:"data"`
	}
	res := Response{}
	err := json.NewDecoder(reader).Decode(&res)
	assert.NoError(t, err)

	err = json.Unmarshal(res.Data, v)
	assert.NoError(t, err)
}

func NewRequest(method, path string, body io.Reader) (*http.Request, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	return req, rec
}

func PostTransaction(t *testing.T, handler http.Handler, tx core.Transaction) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodPost, "/quickstart/transactions", Buffer(t, tx))
	handler.ServeHTTP(rec, req)
	return rec
}

func GetTransaction(handler http.Handler, id int64) *httptest.ResponseRecorder {
	req, rec := NewRequest(http.MethodGet, fmt.Sprintf("/quickstart/transactions/%d", id), nil)
	handler.ServeHTTP(rec, req)
	return rec
}

func WithNewModule(t *testing.T, options ...fx.Option) {
	module := api.Module(api.Config{
		StorageDriver: viper.GetString("sqlite"),
		LedgerLister: controllers.LedgerListerFn(func(r *http.Request) []string {
			return []string{}
		}),
		Version: "latest",
	})
	ch := make(chan struct{})
	options = append([]fx.Option{
		module,
		ledger.ResolveModule(),
		storage.DefaultModule(),
		sqlstorage.TestingModule(),
	}, options...)
	options = append(options, fx.Invoke(func() {
		close(ch)
	}))

	fx.New(options...)
	select {
	case <-ch:
	default:
		assert.Fail(t, "something went wrong")
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
