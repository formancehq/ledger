package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestRunWorkflow(t *testing.T) {
	test(t, func(router *chi.Mux, m *workflow.Manager, db *bun.DB) {
		w, err := m.Create(context.TODO(), workflow.Config{
			Stages: []workflow.Stage{},
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/flows/%s/runs", w.ID), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusCreated, rec.Result().StatusCode)

		req = httptest.NewRequest(http.MethodPost, fmt.Sprintf("/flows/%s/runs", w.ID), nil)
		rec = httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusCreated, rec.Result().StatusCode)
	})
}
