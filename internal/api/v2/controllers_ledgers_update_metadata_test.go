package v2

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
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

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	req := httptest.NewRequest(http.MethodPut, "/"+name+"/metadata", api.Buffer(t, metadata))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
}
