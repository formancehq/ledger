package v2

import (
	"errors"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/api"

	"github.com/formancehq/ledger/internal/api/common"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

// sleep is a simple debug endpoint that blocks for the given duration by
// delegating to the system controller, which in turn executes a
// `SELECT pg_sleep(duration)` query against PostgreSQL.
//
// The duration is provided via the `duration` query parameter and is parsed
// using time.ParseDuration (for example: "1s", "500ms", "2m").
func sleep(systemController systemcontroller.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawDuration := r.URL.Query().Get("duration")
		if rawDuration == "" {
			api.BadRequest(w, common.ErrValidation, errors.New("missing 'duration' query parameter"))
			return
		}

		parsedDuration, err := time.ParseDuration(rawDuration)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if parsedDuration < 0 {
			api.BadRequest(w, common.ErrValidation, errors.New("duration must be non-negative"))
			return
		}

		if err := systemController.Sleep(r.Context(), parsedDuration); err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.NoContent(w)
	}
}


