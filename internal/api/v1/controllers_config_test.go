package v1

import (
	"net/http"
	"net/http/httptest"
	"testing"

	ledger "github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetInfo(t *testing.T) {
	t.Parallel()

	systemController, _ := newTestingSystemController(t, false)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop", testing.Verbose())

	systemController.
		EXPECT().
		ListLedgers(gomock.Any(), gomock.Any()).
		Return(&bunpaginate.Cursor[ledger.Ledger]{
			Data: []ledger.Ledger{
				{
					Name: "a",
				},
				{
					Name: "b",
				},
			},
		}, nil)

	req := httptest.NewRequest(http.MethodGet, "/_info", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	info, _ := api.DecodeSingleResponse[ConfigInfo](t, rec.Body)

	require.EqualValues(t, ConfigInfo{
		Server:  "ledger",
		Version: "develop",
		Config: &LedgerConfig{
			LedgerStorage: &LedgerStorage{
				Driver:  "postgres",
				Ledgers: []string{"a", "b"},
			},
		},
	}, info)
}
