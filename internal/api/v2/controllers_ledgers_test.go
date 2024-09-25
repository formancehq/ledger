package v2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/logging"
	v2 "github.com/formancehq/ledger/v2/internal/api/v2"
	"github.com/formancehq/ledger/v2/internal/opentelemetry/metrics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestUpdateLedgerMetadata(t *testing.T) {
	ctx := logging.TestingContext()

	name := uuid.NewString()
	metadata := map[string]string{
		"foo": "bar",
	}
	backend, _ := newTestingBackend(t, false)
	backend.EXPECT().
		UpdateLedgerMetadata(gomock.Any(), name, metadata).
		Return(nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	req := httptest.NewRequest(http.MethodPut, "/"+name+"/metadata", sharedapi.Buffer(t, metadata))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestDeleteLedgerMetadata(t *testing.T) {
	ctx := logging.TestingContext()

	name := uuid.NewString()
	backend, _ := newTestingBackend(t, false)
	backend.EXPECT().
		DeleteLedgerMetadata(gomock.Any(), name, "foo").
		Return(nil)

	router := v2.NewRouter(backend, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth(), testing.Verbose())

	req := httptest.NewRequest(http.MethodDelete, "/"+name+"/metadata/foo", nil)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}
