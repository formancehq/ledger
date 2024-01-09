package v2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/auth"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetLedger(t *testing.T) {
	t.Parallel()

	b, _ := newTestingBackend(t, false)
	router := v2.NewRouter(b, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

	name := uuid.NewString()
	now := ledger.Now()
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
