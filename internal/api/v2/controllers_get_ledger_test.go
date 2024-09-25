package v2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/ledger/v2/internal/storage/systemstore"

	v2 "github.com/formancehq/ledger/v2/internal/api/v2"
	"github.com/formancehq/ledger/v2/internal/opentelemetry/metrics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetLedger(t *testing.T) {
	t.Parallel()

	b, _ := newTestingBackend(t, false)
	router := v2.NewRouter(b, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	name := uuid.NewString()
	now := time.Now()
	ledger := systemstore.Ledger{
		Name:    name,
		AddedAt: now,
		Bucket:  "bucket0",
	}
	b.
		EXPECT().
		GetLedger(gomock.Any(), name).
		Return(&ledger, nil)

	req := httptest.NewRequest(http.MethodGet, "/"+name, nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	ledgerFromAPI, _ := api.DecodeSingleResponse[systemstore.Ledger](t, rec.Body)
	require.Equal(t, ledger, ledgerFromAPI)
}
