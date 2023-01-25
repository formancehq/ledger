package api

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/orchestration/internal/workflow"
)

func postEventToWorkflowRun(m *workflow.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		event := workflow.NewEmptyEvent()
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			api.BadRequest(w, "VALIDATION", err)
			return
		}

		if err := m.PostEvent(r.Context(), occurrenceId(r), event); err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
