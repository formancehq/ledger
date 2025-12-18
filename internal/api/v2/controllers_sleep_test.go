package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	"go.uber.org/mock/gomock"
)

func TestSleep(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	systemController, _ := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	systemController.EXPECT().
		Sleep(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	t.Run("missing duration", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/sleep", nil)
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid duration", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/sleep?duration=invalid", nil)
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("nominal", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodGet, "/sleep?duration=1ms", nil)
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
	})
}


