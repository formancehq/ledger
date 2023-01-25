package api

import (
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/orchestration/internal/workflow"
)

func readWorkflow(m *workflow.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		workflow, err := m.ReadWorkflow(r.Context(), workflowId(r))
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.Ok(w, workflow)
	}
}
