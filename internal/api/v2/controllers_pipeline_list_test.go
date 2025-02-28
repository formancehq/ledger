package v2

import (
	"github.com/formancehq/go-libs/v2/auth"
	ledger "github.com/formancehq/ledger/internal"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestListPipelines(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	req := httptest.NewRequest(http.MethodGet, "/xxx/pipelines", nil)
	rec := httptest.NewRecorder()

	pipelines := []ledger.Pipeline{
		ledger.NewPipeline(ledger.NewPipelineConfiguration("module1", "connector1")),
		ledger.NewPipeline(ledger.NewPipelineConfiguration("module2", "connector2")),
	}
	systemController.EXPECT().
		ListPipelines(gomock.Any()).
		Return(&bunpaginate.Cursor[ledger.Pipeline]{
			Data: pipelines,
		}, nil)

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
