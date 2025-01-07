package api

import (
	"testing"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/health"
	"github.com/formancehq/go-libs/v2/logging"
)

func newAPI(t *testing.T, backend Backend) *API {
	t.Helper()

	return NewAPI(
		backend,
		health.NewHealthController(),
		auth.NewNoAuth(),
		logging.Testing(),
		api.ServiceInfo{
			Version: "testing",
			Debug:   testing.Verbose(),
		},
	)
}
