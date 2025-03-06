package v2

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
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

	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithConnectors(true))

	req := httptest.NewRequest(http.MethodGet, "/_system/connectors", nil)
	rec := httptest.NewRecorder()
	req = req.WithContext(logging.TestingContext())

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
