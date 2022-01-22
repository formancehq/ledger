package opentelemetry

import (
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"testing"
)

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
