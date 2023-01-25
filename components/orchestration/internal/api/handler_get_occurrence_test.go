package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/formancehq/go-libs/api/apitesting"
	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestGetOccurrence(t *testing.T) {
	test(t, func(router *chi.Mux, m *workflow.Manager, db *bun.DB) {
		w, err := m.Create(context.TODO(), workflow.Config{
			Stages: []workflow.Stage{},
		})
		require.NoError(t, err)

		occurrence, err := m.RunWorkflow(context.TODO(), w.ID, map[string]string{})
		require.NoError(t, err)

		now := time.Now().Round(time.Nanosecond)
		for i := 0; i < 10; i++ {
			_, err := db.NewInsert().Model(&workflow.Status{
				Stage:        i,
				OccurrenceID: occurrence.ID,
				StartedAt:    now,
				TerminatedAt: now.Add(time.Second),
			}).Exec(context.TODO())
			require.NoError(t, err)
		}

		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/flows/%s/runs/%s", w.ID, occurrence.ID), nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Result().StatusCode)
		var retrievedOccurrence workflow.Occurrence
		apitesting.ReadResponse(t, rec, &retrievedOccurrence)
		require.Len(t, retrievedOccurrence.Statuses, 10)
	})
}
