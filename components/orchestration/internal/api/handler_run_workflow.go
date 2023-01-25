package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/orchestration/internal/workflow"
)

func runWorkflow(m *workflow.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		input := make(map[string]string)
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				api.BadRequest(w, "VALIDATION", err)
				return
			}
		}
		occurrence, err := m.RunWorkflow(r.Context(), workflowId(r), input)
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		if wait := strings.ToLower(r.URL.Query().Get("wait")); wait == "true" || wait == "1" {
			ret := struct {
				*workflow.Occurrence
				Error string `json:"error,omitempty"`
			}{
				Occurrence: &occurrence,
			}
			if err := m.Wait(r.Context(), occurrence.WorkflowID, occurrence.ID); err != nil {
				ret.Error = err.Error()
			}
			ret.Occurrence, err = m.GetOccurrence(r.Context(), occurrence.WorkflowID, occurrence.ID)
			if err != nil {
				panic(err)
			}
			api.Created(w, ret)
			return
		}

		api.Created(w, occurrence)
	}
}
