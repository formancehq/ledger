package server

import (
	"os"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/webhooks/pkg/httpserver"
	"github.com/formancehq/webhooks/pkg/storage/postgres"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func StartModule(addr string) fx.Option {
	var options []fx.Option

	options = append(options, otlptraces.CLITracesModule(viper.GetViper()))

	options = append(options, fx.Provide(
		func() string { return addr },
		httpserver.NewMuxServer,
		postgres.NewStore,
		newServerHandler,
	))
	options = append(options, fx.Invoke(httpserver.RegisterHandler))
	options = append(options, fx.Invoke(httpserver.Run))

	logging.Debugf("starting server with env:")
	for _, e := range os.Environ() {
		logging.Debugf("%s", e)
	}

	return fx.Module("webhooks server", options...)
}
