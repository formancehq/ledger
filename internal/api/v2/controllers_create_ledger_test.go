package v2_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/formancehq/ledger/internal/api/v2"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/internal/storage/driver"
)

func TestConfigureLedger(t *testing.T) {
	t.Parallel()

	type testCase struct {
		configuration driver.LedgerConfiguration
		name          string
	}

	testCases := []testCase{
		{
			name:          "nominal",
			configuration: driver.LedgerConfiguration{},
		},
		{
			name: "with alternative bucket",
			configuration: driver.LedgerConfiguration{
				Bucket: "bucket0",
			},
		},
		{
			name: "with metadata",
			configuration: driver.LedgerConfiguration{
				Metadata: map[string]string{
					"foo": "bar",
				},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newTestingBackend(t, false)
			router := v2.NewRouter(b, nil, metrics.NewNoOpRegistry(), auth.NewNoAuth())

			name := uuid.NewString()
			b.
				EXPECT().
				CreateLedger(gomock.Any(), name, testCase.configuration).
				Return(nil)

			req := httptest.NewRequest(http.MethodPost, "/"+name, api.Buffer(t, testCase.configuration))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}
