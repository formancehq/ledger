package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListConnectors(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	systemController.EXPECT().
		ListConnectors(gomock.Any()).
		Return(&bunpaginate.Cursor[ledger.Connector]{
			Data: []ledger.Connector{
				ledger.NewConnector(ledger.NewConnectorConfiguration("connector1", json.RawMessage(`{}`))),
				ledger.NewConnector(ledger.NewConnectorConfiguration("connector2", json.RawMessage(`{}`))),
			},
		}, nil)

	router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")

	req := httptest.NewRequest(http.MethodGet, "/_system/connectors", nil)
	rec := httptest.NewRecorder()
	req = req.WithContext(logging.TestingContext())

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
