package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	stdtime "time"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/api/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetTransactionsSummary(t *testing.T) {
	t.Parallel()

	t.Run("success with single asset", func(t *testing.T) {
		t.Parallel()

		// Setup test data
		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Mock the GetTransactionsSummary call
		mockLedgerController.EXPECT().
			GetTransactionsSummary(gomock.Any(), "expenses:salary").
			Return([]ledgerstore.TransactionsSummary{
				{
					Asset: "USD",
					Count: 2,
					Sum:   "500", // 1000 (from world) - 500 (to bank) = 500
				},
			}, nil)

		// Create test server with mock controller
		server := newTestServer(t, mockLedgerController)

		// Create request with proper pagination parameters
		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary", nil)
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
			Data []summaryResponse `json:"data"`
		}
		err = json.Unmarshal([]byte(body), &responseWrapper)
		require.NoError(t, err, "Failed to unmarshal response: %s", body)

		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "expenses:salary", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)
		require.Equal(t, int64(2), responseWrapper.Data[0].Count)
		require.Equal(t, int64(500), responseWrapper.Data[0].Sum.Int64()) // 1000 - 500 = 500
	})

	t.Run("with 1000 transactions", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Calculate expected sum: 1 + 2 + ... + 1000 = 1000*1001/2 = 500500
		expectedSum := big.NewInt(500500)

		// Mock the GetTransactionsSummary call
		mockLedgerController.EXPECT().
			GetTransactionsSummary(gomock.Any(), "test:account").
			Return([]ledgerstore.TransactionsSummary{
				{
					Asset: "USD",
					Count: 1000,
					Sum:   expectedSum.String(),
				},
			}, nil)

		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=test:account&asset=USD", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)

		var responseWrapper struct {
			Data []summaryResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err)

		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "test:account", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)
		require.Equal(t, int64(1000), responseWrapper.Data[0].Count)

		require.Equal(t, 0, responseWrapper.Data[0].Sum.Cmp(expectedSum),
			"expected sum %s, got %s", expectedSum, responseWrapper.Data[0].Sum)
	})

	t.Run("missing account parameter", func(t *testing.T) {
		t.Parallel()

		// Create test server with nil controller since we expect to fail before any controller call
		server := newTestServer(t, nil)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("success with multiple transactions", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		mockLedgerController.EXPECT().
			GetTransactionsSummary(gomock.Any(), "expenses:salary").
			Return([]ledgerstore.TransactionsSummary{
				{
					Asset: "USD",
					Count: 3,
					Sum:   "700", // 1000 (from world) - 500 (to bank) + 200 (from client) = 700
				},
			}, nil)

		// Create test server with mock controller
		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		// Verify response
		require.Equal(t, http.StatusOK, rr.Code)

		// Parse response
		var responseWrapper struct {
			Data []summaryResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err, "Failed to unmarshal response")

		// Verify the response contains the expected data
		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "expenses:salary", responseWrapper.Data[0].Account)
		require.Equal(t, "USD", responseWrapper.Data[0].Asset)
		require.Equal(t, int64(3), responseWrapper.Data[0].Count)
		// 1000 (from world) - 500 (to bank) + 200 (from client) = 700
		require.Equal(t, int64(700), responseWrapper.Data[0].Sum.Int64())
	})

	t.Run("success with time range", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		start := libtime.Now().Add(-libtime.Hour)
		end := libtime.Now()

		mockLedgerController.EXPECT().
			GetTransactionsSummaryWithTimeRange(gomock.Any(), "expenses:salary", gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, account string, startTime, endTime *libtime.Time) ([]ledgerstore.TransactionsSummary, error) {
				require.Equal(t, "expenses:salary", account)
				require.WithinDuration(t, start.Time, startTime.Time, stdtime.Second)
				require.WithinDuration(t, end.Time, endTime.Time, stdtime.Second)
				return []ledgerstore.TransactionsSummary{
					{
						Asset: "USD",
						Count: 1,
						Sum:   "100",
					},
				}, nil
			})

		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary&start_time="+start.Format(libtime.RFC3339Nano)+"&end_time="+end.Format(libtime.RFC3339Nano), nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)

		var responseWrapper struct {
			Data []summaryResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err)
		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, int64(1), responseWrapper.Data[0].Count)
		require.Equal(t, int64(100), responseWrapper.Data[0].Sum.Int64())
	})

	t.Run("asset filter selects only requested asset", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		mockLedgerController.EXPECT().
			GetTransactionsSummary(gomock.Any(), "expenses:salary").
			Return([]ledgerstore.TransactionsSummary{
				{Asset: "USD", Count: 1, Sum: "50"},
				{Asset: "EUR", Count: 2, Sum: "75"},
			}, nil)

		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary&asset=EUR", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)

		var responseWrapper struct {
			Data []summaryResponse `json:"data"`
		}
		err = json.Unmarshal(rr.Body.Bytes(), &responseWrapper)
		require.NoError(t, err)
		require.Len(t, responseWrapper.Data, 1)
		require.Equal(t, "EUR", responseWrapper.Data[0].Asset)
		require.Equal(t, int64(2), responseWrapper.Data[0].Count)
		require.Equal(t, int64(75), responseWrapper.Data[0].Sum.Int64())
	})

	t.Run("invalid time format returns bad request", func(t *testing.T) {
		t.Parallel()

		server := newTestServer(t, nil)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary&start_time=not-a-time", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("controller error returns internal server error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		mockLedgerController.EXPECT().
			GetTransactionsSummary(gomock.Any(), "expenses:salary").
			Return(nil, fmt.Errorf("backend error"))

		server := newTestServer(t, mockLedgerController)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		require.Equal(t, http.StatusInternalServerError, rr.Code)
	})

	t.Run("start time after end time returns bad request", func(t *testing.T) {
		t.Parallel()

		server := newTestServer(t, nil)

		req, err := http.NewRequest(http.MethodGet, "/transactions/summary?account=expenses:salary&start_time=2024-01-02T00:00:00Z&end_time=2024-01-01T00:00:00Z", nil)
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
	router.Get("/transactions/summary", getTransactionsSummary)

	// Add middleware to inject the mock controller into the request context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := common.ContextWithLedger(r.Context(), mockController)
		router.ServeHTTP(w, r.WithContext(ctx))
	})
}
