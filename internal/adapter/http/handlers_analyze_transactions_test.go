package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleAnalyzeTransactions_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			require.Equal(t, "my-ledger", ledgerName)
			require.Equal(t, uint32(0), variableThreshold)

			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 100,
				TotalReverted:     5,
				FlowPatterns: []*servicepb.FlowPattern{
					{
						Signature:        "world->bank:main[USD]",
						Structure:        servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE,
						TransactionCount: 80,
						Postings: []*servicepb.NormalizedPosting{
							{SourcePattern: "world", DestinationPattern: "bank:main", Asset: "USD"},
						},
						Temporal: &servicepb.TemporalStats{
							FirstSeen:          &commonpb.Timestamp{Data: 1000000},
							LastSeen:           &commonpb.Timestamp{Data: 2000000},
							TransactionsPerDay: 40.0,
							PeakHours: []*servicepb.HourBucket{
								{Hour: 14, Count: 20},
							},
						},
						VolumeStats: []*servicepb.AssetVolumeStats{
							{Asset: "USD", TotalVolume: "10000", AverageVolume: "125", MinVolume: "10", MaxVolume: "500", TransactionCount: 80},
						},
						MetadataKeys: []string{"category"},
					},
				},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[analyzeTransactionsResponseJSON]](t, w)
	resp := wrapper.Data
	require.Equal(t, uint64(100), resp.TotalTransactions)
	require.Equal(t, uint64(5), resp.TotalReverted)
	require.Len(t, resp.FlowPatterns, 1)

	fp := resp.FlowPatterns[0]
	require.Equal(t, "world->bank:main[USD]", fp.Signature)
	require.Equal(t, "simple", fp.Structure)
	require.Equal(t, uint64(80), fp.TransactionCount)
	require.Len(t, fp.Postings, 1)
	require.Equal(t, "world", fp.Postings[0].SourcePattern)
	require.Equal(t, "bank:main", fp.Postings[0].DestinationPattern)
	require.NotNil(t, fp.Temporal)
	require.Equal(t, float64(40.0), fp.Temporal.TransactionsPerDay)
	require.Len(t, fp.VolumeStats, 1)
	require.Equal(t, "10000", fp.VolumeStats[0].TotalVolume)
	require.Equal(t, []string{"category"}, fp.MetadataKeys)
}

func TestHandleAnalyzeTransactions_WithThreshold(t *testing.T) {
	t.Parallel()

	var capturedThreshold uint32

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, variableThreshold uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			capturedThreshold = variableThreshold

			return &servicepb.AnalyzeTransactionsResponse{}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions?variableThreshold=25", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, uint32(25), capturedThreshold)
}

func TestHandleAnalyzeTransactions_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/analyze-transactions", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAnalyzeTransactions_InvalidThreshold(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions?variableThreshold=abc", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
	require.Contains(t, resp.ErrorMessage, "variableThreshold")
}

func TestHandleAnalyzeTransactions_BackendError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, _ uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, errors.New("internal error")
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAnalyzeTransactions_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, _ uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/analyze-transactions", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleAnalyzeTransactions_EmptyResponse(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, _ uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 0,
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/empty-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "empty-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	wrapper := decodeResponse[BaseResponse[analyzeTransactionsResponseJSON]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.TotalTransactions)
	require.Empty(t, wrapper.Data.FlowPatterns)
}

func TestHandleAnalyzeTransactions_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, _ uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, commonpb.ErrNoLeader
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestHandleAnalyzeTransactions_FullRouteIntegration tests that the route is correctly
// registered in NewHandler and accessible via a full HTTP request.
func TestHandleAnalyzeTransactions_FullRouteIntegration(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		analyzeTransactionsFn: func(_ context.Context, _ string, _ uint32) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 5,
				FlowPatterns:      []*servicepb.FlowPattern{},
			}, nil
		},
	}

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/my-ledger/analyze-transactions", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}
