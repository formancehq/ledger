package v1

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetLedgerInfo(t *testing.T) {
	t.Parallel()

	systemController, mock := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

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

	mock.EXPECT().
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
