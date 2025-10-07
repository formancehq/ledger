package v2

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
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

		// Mock the GetTransactionsSum call
		mockLedgerController.EXPECT().
			GetTransactionsSum(gomock.Any(), "expenses:salary").
			Return([]ledgerstore.TransactionsSum{
				{
					Asset: "USD",
					Sum:   "500", // 1000 (from world) - 500 (to bank) = 500
				},
			}, nil)

		// Create test server with mock controller
		server := newTestServer(t, mockLedgerController)

		// Create request with proper pagination parameters
		req, err := http.NewRequest(http.MethodGet, "/transactions/sum?account=expenses:salary", nil)
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

	t.Run("with 1000 transactions", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Calculate expected sum: 1 + 2 + ... + 1000 = 1000*1001/2 = 500500
		expectedSum := big.NewInt(500500)

		// Mock the GetTransactionsSum call
		mockLedgerController.EXPECT().
			GetTransactionsSum(gomock.Any(), "test:account").
			Return([]ledgerstore.TransactionsSum{
				{
					Asset: "USD",
					Sum:   expectedSum.String(),
				},
			}, nil)

		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/sum?account=test:account&asset=USD", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)

		var responseWrapper struct {
			Data []sumResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err)

		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "test:account", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)

		require.Equal(t, 0, responseWrapper.Data[0].Sum.Cmp(expectedSum),
			"expected sum %s, got %s", expectedSum, responseWrapper.Data[0].Sum)
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

	t.Run("pagination with multiple pages", func(t *testing.T) {
		t.Parallel()

		// Setup test data
		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Mock the GetTransactionsSum call
		mockLedgerController.EXPECT().
			GetTransactionsSum(gomock.Any(), "expenses:salary").
			Return([]ledgerstore.TransactionsSum{
				{
					Asset: "USD",
					Sum:   "700", // 1000 (from world) - 500 (to bank) + 200 (from client) = 700
				},
			}, nil)

		// Create test server with mock controller
		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/sum?account=expenses:salary", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		// Verify response
		require.Equal(t, http.StatusOK, rr.Code)

		// Parse response
		var responseWrapper struct {
			Data []sumResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err, "Failed to unmarshal response")

		// Verify the response contains the expected data
		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "expenses:salary", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)
		// 1000 (from world) - 500 (to bank) + 200 (from client) = 700
		require.Equal(t, int64(700), responseWrapper.Data[0].Sum.Int64())
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

type paginatedQueryMatcher struct {
	expected storagecommon.InitialPaginatedQuery[any]
}

func (m *paginatedQueryMatcher) Matches(x interface{}) bool {
	actual, ok := x.(storagecommon.InitialPaginatedQuery[any])
	if !ok {
		return false
	}
	return reflect.DeepEqual(actual, m.expected)
}

func (m *paginatedQueryMatcher) String() string {
	return fmt.Sprintf("matches: %+v", m.expected)
}
