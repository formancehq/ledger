package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersInfo(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

	migrationInfo := []migrations.Info{
		{
			Version: "1",
			Name:    "init",
			State:   "ready",
			Date:    time.Now().Add(-2 * time.Minute).Round(time.Second).UTC(),
		},
		{
			Version: "2",
			Name:    "fix",
			State:   "ready",
			Date:    time.Now().Add(-time.Minute).Round(time.Second).UTC(),
		},
	}

	ledgerController.EXPECT().
		GetMigrationsInfo(gomock.Any()).
		Return(migrationInfo, nil)

	req := httptest.NewRequest(http.MethodGet, "/xxx/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info, ok := api.DecodeSingleResponse[Info](t, rec.Body)
	require.True(t, ok)

	require.EqualValues(t, Info{
		Name: "xxx",
		Storage: StorageInfo{
			Migrations: migrationInfo,
		},
	}, info)
}
