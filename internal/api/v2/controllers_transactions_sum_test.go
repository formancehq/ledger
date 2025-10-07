package v2

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetTransactionsSum(t *testing.T) {
	t.Parallel()

	t.Run("success with single asset", func(t *testing.T) {
		t.Parallel()

		// Setup test data
		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Mock the ListTransactions call and create expected query with the same parameters as the actual request
		desc := bunpaginate.OrderDesc
		order := bunpaginate.Order(desc)
		expectedQuery := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 15,
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand:  []string{},
				PIT:     nil,
				OOT:     nil,
				Builder: nil,
				Opts:    nil,
			},
		}

		mockLedgerController.EXPECT().
			ListTransactions(gomock.Any(), matchPaginatedQuery(expectedQuery)).
			Return(&bunpaginate.Cursor[ledger.Transaction]{
				Data: []ledger.Transaction{
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("world", "expenses:salary", "USD", big.NewInt(1000)),
					),
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("expenses:salary", "bank:checking", "USD", big.NewInt(500)),
					),
				},
			}, nil)

		// Create test server with mock controller
		server := newTestServer(t, mockLedgerController)

		// Create request with proper pagination parameters
		req, err := http.NewRequest(http.MethodGet, "/transactions/sum?account=expenses:salary&pageSize=15", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		// Execute request
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		// Verify response
		require.Equal(t, http.StatusOK, rr.Code)

		// Print response body for debugging
		body := rr.Body.String()
		t.Logf("Response body: %s", body)

		// Define a struct to match the actual response format
		var responseWrapper struct {
			Data []sumResponse `json:"data"`
		}
		err = json.Unmarshal([]byte(body), &responseWrapper)
		require.NoError(t, err, "Failed to unmarshal response: %s", body)

		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "expenses:salary", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)
		require.Equal(t, int64(500), responseWrapper.Data[0].Sum.Int64()) // 1000 - 500 = 500
	})

	t.Run("missing account parameter", func(t *testing.T) {
		t.Parallel()

		// Create test server with nil controller since we expect to fail before any controller call
		server := newTestServer(t, nil)

		req, err := http.NewRequest(http.MethodGet, "/transactions/sum", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// newTestServer creates a test server with the provided mock controller
func newTestServer(t *testing.T, mockController *LedgerController) http.Handler {
	t.Helper()

	// Create a new router with the test dependencies
	router := chi.NewRouter()
	router.Get("/transactions/sum", getTransactionsSum)

	// Add middleware to inject the mock controller into the request context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := common.ContextWithLedger(r.Context(), mockController)
		router.ServeHTTP(w, r.WithContext(ctx))
	})
}

// matchPaginatedQuery is a gomock matcher for paginated queries
func matchPaginatedQuery(expected storagecommon.InitialPaginatedQuery[any]) gomock.Matcher {
	return gomock.GotFormatterAdapter(
		gomock.GotFormatterFunc(func(actual interface{}) string {
			return ""
		}),
		gomock.Eq(expected),
	)
}
