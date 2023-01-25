package api

import (
	"net/http"

	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
)

func newRouter(m *workflow.Manager) *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Plug middleware to handle traces
	r.Use(otelchi.Middleware("orchestration"))
	r.Route("/flows", func(r chi.Router) {
		r.Get("/", listWorkflows(m))
		r.Post("/", createWorkflow(m))
		r.Route("/{workflowId}", func(r chi.Router) {
			r.Get("/", readWorkflow(m))
			r.Route("/runs", func(r chi.Router) {
				r.Post("/", runWorkflow(m))
				r.Get("/", listOccurrences(m))
				r.Route("/{occurrenceId}", func(r chi.Router) {
					r.Get("/", readOccurrence(m))
					r.Post("/events", postEventToWorkflowRun(m))
					r.Put("/abort", abortWorkflowRun(m))
				})
			})
		})
	})
	return r
}

func workflowId(r *http.Request) string {
	return chi.URLParam(r, "workflowId")
}

func occurrenceId(r *http.Request) string {
	return chi.URLParam(r, "occurrenceId")
}
