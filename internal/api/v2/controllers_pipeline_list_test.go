package v2

import (
	"github.com/formancehq/go-libs/v3/auth"
	ledger "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListPipelines(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", WithExporters(true))

	req := httptest.NewRequest(http.MethodGet, "/xxx/pipelines", nil)
	rec := httptest.NewRecorder()

	pipelines := []ledger.Pipeline{
		ledger.NewPipeline(ledger.NewPipelineConfiguration("module1", "exporter1")),
		ledger.NewPipeline(ledger.NewPipelineConfiguration("module2", "exporter2")),
	}
	systemController.EXPECT().
		ListPipelines(gomock.Any()).
		Return(&bunpaginate.Cursor[ledger.Pipeline]{
			Data: pipelines,
		}, nil)

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
