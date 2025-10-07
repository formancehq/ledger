package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
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

		// Mock the ListTransactions call and create expected query with the fixed page size of 100
		desc := bunpaginate.OrderDesc
		order := bunpaginate.Order(desc)
		expectedQuery := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 100, // Fixed page size for internal pagination
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

		// Mock the response with HasMore: false to indicate this is the only page
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
				HasMore: false, // Indicate this is the last page
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

		// Mock the first page of transactions
		desc := bunpaginate.OrderDesc
		order := bunpaginate.Order(desc)
		expectedQuery1 := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 100, // Fixed page size for internal pagination
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{},
			},
		}

		// First page with 2 transactions and hasMore=true
		mockLedgerController.EXPECT().
			ListTransactions(gomock.Any(), matchPaginatedQuery(expectedQuery1)).
			Return(&bunpaginate.Cursor[ledger.Transaction]{
				Data: []ledger.Transaction{
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("world", "expenses:salary", "USD", big.NewInt(1000)),
					),
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("expenses:salary", "bank:checking", "USD", big.NewInt(500)),
					),
				},
				HasMore: true,
				Next:     "next-page-cursor",
			}, nil)

		// Second page with 1 more transaction and hasMore=false
		expectedQuery2 := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 100, // Same fixed page size
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{},
			},
		}

		mockLedgerController.EXPECT().
			ListTransactions(gomock.Any(), matchPaginatedQuery(expectedQuery2)).
			Return(&bunpaginate.Cursor[ledger.Transaction]{
				Data: []ledger.Transaction{
					ledger.NewTransaction().WithPostings(
						ledger.NewPosting("client:1", "expenses:salary", "USD", big.NewInt(200)),
					),
				},
				HasMore: false, // Last page
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

// matchPaginatedQuery is a gomock matcher for paginated queries
func matchPaginatedQuery(expected storagecommon.InitialPaginatedQuery[any]) gomock.Matcher {
	return &paginatedQueryMatcher{expected: expected}
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

func TestGetPaginatedTransactions(t *testing.T) {
	t.Parallel()

	// Define test constants
	const (
		testAccount    = "expenses:salary"
		contentType    = "Content-Type"
		appJSON        = "application/json"
		transactionsEP = "/transactions/sum"
		nextPageCursor = "next-page-cursor"
	)

	t.Run("successful first page", func(t *testing.T) {
		t.Parallel()

		// Setup test data
		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Create a test request with pagination parameters
		req, err := http.NewRequest(http.MethodGet, transactionsEP, nil)
		req.Header.Set(contentType, appJSON)
		require.NoError(t, err)

		// Create a response recorder to capture the response
		rr := httptest.NewRecorder()

		// Mock the expected ListTransactions call
		desc := bunpaginate.OrderDesc
		order := bunpaginate.Order(desc)
		expectedQuery := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 100, // Default page size from the function parameter
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{},
			},
		}

		// Mock the response
		expectedTransactions := []ledger.Transaction{
			ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", testAccount, "USD", big.NewInt(1000)),
			),
		}

		mockLedgerController.EXPECT().
			ListTransactions(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx interface{}, q storagecommon.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
				// Verify the query parameters match what we expect
				reqQuery, ok := q.(storagecommon.InitialPaginatedQuery[any])
				require.True(t, ok, "Expected InitialPaginatedQuery")
				require.Equal(t, expectedQuery.PageSize, reqQuery.PageSize)
				require.Equal(t, expectedQuery.Column, reqQuery.Column)
				require.Equal(t, expectedQuery.Order, reqQuery.Order)
				require.Equal(t, expectedQuery.Options.Expand, reqQuery.Options.Expand)

				return &bunpaginate.Cursor[ledger.Transaction]{
					Data:    expectedTransactions,
					HasMore: true,
					Next:    nextPageCursor,
				}, nil
			})

		// Call the function with the default page size
		cursor, ok := getPaginatedTransactions(rr, req, mockLedgerController, 100)

		// Verify the results
		require.True(t, ok, "Expected getPaginatedTransactions to succeed")
		require.NotNil(t, cursor, "Expected non-nil cursor")
		require.True(t, cursor.HasMore, "Expected HasMore to be true")
		require.Equal(t, nextPageCursor, cursor.Next, "Unexpected next cursor value")
		require.Len(t, cursor.Data, 1, "Expected one transaction")
	})

	t.Run("error from ListTransactions", func(t *testing.T) {
		t.Parallel()

		// Setup test data
		ctrl := gomock.NewController(t)
		mockLedgerController := NewLedgerController(ctrl)

		// Create a test request
		req, err := http.NewRequest(http.MethodGet, "/transactions/sum", nil)
		req.Header.Set("Content-Type", "application/json")
		require.NoError(t, err)

		// Create a response recorder to capture the response
		rr := httptest.NewRecorder()

		// Mock the ListTransactions call to return an error
		desc := bunpaginate.OrderDesc
		order := bunpaginate.Order(desc)
		expectedQuery := storagecommon.InitialPaginatedQuery[any]{
			PageSize: 100, // Default page size
			Column:   "timestamp",
			Order:    &order,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{},
			},
		}

		expectedError := errors.New("database error")
		mockLedgerController.EXPECT().
			ListTransactions(gomock.Any(), matchPaginatedQuery(expectedQuery)).
			Return(nil, expectedError)

		// Call the function
		cursor, ok := getPaginatedTransactions(rr, req, mockLedgerController, 100)

		// Verify the results
		require.False(t, ok, "Expected getPaginatedTransactions to fail")
		require.Nil(t, cursor, "Expected nil cursor on error")
	})
}
