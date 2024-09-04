package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLedgersUpdateMetadata(t *testing.T) {
	ctx := logging.TestingContext()

	name := uuid.NewString()
	metadata := map[string]string{
		"foo": "bar",
	}
	systemController, _ := newTestingSystemController(t, false)
	systemController.EXPECT().
		UpdateLedgerMetadata(gomock.Any(), name, metadata).
		Return(nil)

	router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

	req := httptest.NewRequest(http.MethodPut, "/"+name+"/metadata", api.Buffer(t, metadata))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}
