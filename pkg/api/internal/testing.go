package internal

import (
	"bytes"
	"encoding/json"
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

func Buffer(t *testing.T, v interface{}) *bytes.Buffer {
	data, err := json.Marshal(v)
	assert.NoError(t, err)
	return bytes.NewBuffer(data)
}

func NewRequest(t *testing.T, method, path string, body io.Reader) (*http.Request, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/quickstart/transactions", body)
	req.Header.Set("Content-Type", "application/json")
	return req, rec
}

func PostTransaction(t *testing.T, handler http.Handler, tx core.Transaction) *httptest.ResponseRecorder {
	req, rec := NewRequest(t, http.MethodPost, "/quickstart/transactions", Buffer(t, tx))
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

func Run(t *testing.T, name string, fn interface{}) {
	t.Run(name, func(t *testing.T) {
		WithNewModule(t, fx.Invoke(fn))
	})
}
