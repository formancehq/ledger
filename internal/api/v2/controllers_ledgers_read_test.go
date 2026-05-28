package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
)

func TestLedgersRead(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

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
