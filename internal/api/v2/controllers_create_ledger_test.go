package v2_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/ledger/internal/storage/driver"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConfigureLedger(t *testing.T) {
	t.Parallel()

	b, _ := newTestingBackend(t, false)
	router := v2.NewRouter(b, nil, metrics.NewNoOpRegistry())

	name := uuid.NewString()
	b.
		EXPECT().
		CreateLedger(gomock.Any(), name, driver.LedgerConfiguration{
			Bucket: "bucket0",
		}).
		Return(nil)

	req := httptest.NewRequest(http.MethodPost, "/"+name, bytes.NewBufferString(`{"bucket": "bucket0"}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}
