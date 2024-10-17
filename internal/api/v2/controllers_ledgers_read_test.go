package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/auth"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersRead(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), testing.Verbose())

	name := uuid.NewString()
	now := time.Now()
	l := ledger.Ledger{
		Name:    name,
		AddedAt: now,
		Configuration: ledger.Configuration{
			Bucket: "bucket0",
		},
	}
	systemController.
		EXPECT().
		GetLedger(gomock.Any(), name).
		Return(&l, nil)

	req := httptest.NewRequest(http.MethodGet, "/"+name, nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	ledgerFromAPI, _ := api.DecodeSingleResponse[ledger.Ledger](t, rec.Body)
	require.Equal(t, l, ledgerFromAPI)
}
