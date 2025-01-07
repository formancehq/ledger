package v2

import (
	"github.com/formancehq/go-libs/v2/auth"
	ingester "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListPipelines(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), os.Getenv("DEBUG") == "true")

	req := httptest.NewRequest(http.MethodGet, "/xxx/pipelines", nil)
	rec := httptest.NewRecorder()

	pipelines := []ingester.Pipeline{
		ingester.NewPipeline(ingester.NewPipelineConfiguration("module1", "connector1"), ingester.NewReadyState()),
		ingester.NewPipeline(ingester.NewPipelineConfiguration("module2", "connector2"), ingester.NewReadyState()),
	}
	ledgerController.EXPECT().
		ListPipelines(gomock.Any()).
		Return(&bunpaginate.Cursor[ingester.Pipeline]{
			Data: pipelines,
		}, nil)

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
