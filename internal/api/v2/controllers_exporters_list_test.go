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

func TestListExporters(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	systemController.EXPECT().
		ListExporters(gomock.Any()).
		Return(&bunpaginate.Cursor[ledger.Exporter]{
			Data: []ledger.Exporter{
				ledger.NewExporter(ledger.NewExporterConfiguration("exporter1", json.RawMessage(`{}`))),
				ledger.NewExporter(ledger.NewExporterConfiguration("exporter2", json.RawMessage(`{}`))),
			},
		}, nil)

	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

	req := httptest.NewRequest(http.MethodGet, "/_/exporters", nil)
	rec := httptest.NewRecorder()
	req = req.WithContext(logging.TestingContext())

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
