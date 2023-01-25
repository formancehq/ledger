package api

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

func createWorkflow(m *workflow.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		config := workflow.Config{}
		if r.Header.Get("Content-Type") == "text/vnd.yaml" {
			payload := make(map[string]any)
			if err := yaml.NewDecoder(r.Body).Decode(&payload); err != nil {
				api.BadRequest(w, "VALIDATION", errors.Wrap(err, "unmarshalling yaml"))
				return
			}

			asJson, err := json.Marshal(payload)
			if err != nil {
				panic(err)
			}

			if err := json.Unmarshal(asJson, &config); err != nil {
				api.BadRequest(w, "VALIDATION", errors.Wrap(err, "unmarshalling workflow data"))
				return
			}
		} else {
			if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
				api.BadRequest(w, "VALIDATION", errors.Wrap(err, "unmarshalling json body"))
				return
			}
		}

		workflow, err := m.Create(r.Context(), config)
		if err != nil {
			api.InternalServerError(w, r, errors.Wrap(err, "creating workflow"))
			return
		}

		api.Created(w, workflow)
	}
}
