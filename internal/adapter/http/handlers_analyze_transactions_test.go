package http

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleAnalyzeTransactions_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ledgerName string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
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
							FirstSeen:          &ledgerpb.Timestamp{Data: 1000000},
							LastSeen:           &ledgerpb.Timestamp{Data: 2000000},
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
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Decode monetary leaves independently from the production protobuf codecs.
	wrapper := decodeResponse[BaseResponse[struct {
		TotalTransactions uint64 `json:"totalTransactions"`
		TotalReverted     uint64 `json:"totalReverted"`
		FlowPatterns      []struct {
			Signature        string                   `json:"signature"`
			Structure        string                   `json:"structure"`
			TransactionCount uint64                   `json:"transactionCount"`
			Postings         []*normalizedPostingJSON `json:"postings"`
			Temporal         *temporalStatsJSON       `json:"temporal"`
			VolumeStats      []struct {
				TotalVolume stdjson.RawMessage `json:"totalVolume"`
			} `json:"volumeStats"`
			MetadataKeys []string `json:"metadataKeys"`
		} `json:"flowPatterns"`
	}]](t, w)
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
	require.Equal(t, "10000", string(fp.VolumeStats[0].TotalVolume))
	require.Equal(t, []string{"category"}, fp.MetadataKeys)
}

func TestHandleAnalyzeTransactions_WithThreshold(t *testing.T) {
	t.Parallel()

	var capturedThreshold uint32

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, variableThreshold uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			capturedThreshold = variableThreshold

			return &servicepb.AnalyzeTransactionsResponse{}, nil
		}).AnyTimes()
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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/analyze-transactions", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAnalyzeTransactions_InvalidThreshold(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, errors.New("internal error")
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 0,
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/empty-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "empty-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Decode monetary leaves independently from the production protobuf codecs.
	wrapper := decodeResponse[BaseResponse[struct {
		TotalTransactions uint64 `json:"totalTransactions"`
		TotalReverted     uint64 `json:"totalReverted"`
		FlowPatterns      []struct {
			Signature        string                   `json:"signature"`
			Structure        string                   `json:"structure"`
			TransactionCount uint64                   `json:"transactionCount"`
			Postings         []*normalizedPostingJSON `json:"postings"`
			Temporal         *temporalStatsJSON       `json:"temporal"`
			VolumeStats      []struct {
				TotalVolume stdjson.RawMessage `json:"totalVolume"`
			} `json:"volumeStats"`
			MetadataKeys []string `json:"metadataKeys"`
		} `json:"flowPatterns"`
	}]](t, w)
	require.Equal(t, uint64(0), wrapper.Data.TotalTransactions)
	require.Empty(t, wrapper.Data.FlowPatterns)
}

// TestHandleAnalyzeTransactions_NoNullCollections pins the fix for a real
// conformance failure: the OpenAPI schema types metadataKeys as a
// non-nullable array, but FlowPattern.GetMetadataKeys() returns nil for an
// absent repeated field, which serialized as JSON null. Left unset here to
// reproduce that case, mirroring TestTransaction_MarshalJSON_NoNullCollections.
func TestHandleAnalyzeTransactions_NoNullCollections(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 1,
				FlowPatterns: []*servicepb.FlowPattern{
					{
						Signature:        "world->alice[USD/2]",
						Structure:        servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE,
						TransactionCount: 1,
						Postings: []*servicepb.NormalizedPosting{
							{SourcePattern: "world", DestinationPattern: "alice", Asset: "USD/2"},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{
		"ledgerName": "my-ledger",
	})

	srv.handleAnalyzeTransactions(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	require.Contains(t, body, `"metadataKeys":[]`)
	require.NotContains(t, body, `"metadataKeys":null`)
}

func TestHandleAnalyzeTransactions_NoLeaderError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return nil, ledgerpb.ErrNoLeader
		}).AnyTimes()
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

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			return &servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 5,
				FlowPatterns:      []*servicepb.FlowPattern{},
			}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v3/my-ledger/analyze-transactions", nil)

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleAnalyzeTransactions_MonetaryEncoding(t *testing.T) {
	t.Parallel()
	const amount = "9007199254740993"
	for _, header := range []string{"", "true"} {
		t.Run(header, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AnalyzeTransactions(gomock.Any(), "my-ledger", uint32(0), gomock.Any()).Return(&servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 1,
				FlowPatterns: []*servicepb.FlowPattern{{TransactionCount: 1, VolumeStats: []*servicepb.AssetVolumeStats{{
					Asset: "USD", TotalVolume: amount, AverageVolume: amount, MinVolume: amount, MaxVolume: amount, TransactionCount: 1,
				}}}},
			}, nil)
			r := newRequest(t, http.MethodGet, "/my-ledger/analyze-transactions", nil, map[string]string{"ledgerName": "my-ledger"})
			r.Header.Set(HeaderBigIntAsString, header)
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleAnalyzeTransactions(w, r)
			require.Equal(t, http.StatusOK, w.Code)
			for _, field := range []string{"totalVolume", "averageVolume", "minVolume", "maxVolume"} {
				if header == "true" {
					require.Contains(t, w.Body.String(), `"`+field+`":"`+amount+`"`)
				} else {
					require.Contains(t, w.Body.String(), `"`+field+`":`+amount)
				}
			}
			require.Contains(t, w.Body.String(), `"transactionCount":1`)
			require.Equal(t, byte('\n'), w.Body.Bytes()[w.Body.Len()-1], "preserve the analysis streaming newline")
		})
	}
}

func TestHandleAnalyzeTransactions_TemporalTimestamps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		firstSeen *commonpb.Timestamp
		lastSeen  *commonpb.Timestamp
		want      string
	}{
		{
			name:      "fractional microseconds",
			firstSeen: &commonpb.Timestamp{Data: 1767225600123456},
			lastSeen:  &commonpb.Timestamp{Data: 1767225600000001},
			want:      `"firstSeen":"2026-01-01T00:00:00.123456Z","lastSeen":"2026-01-01T00:00:00.000001Z"`,
		},
		{
			name:      "whole seconds",
			firstSeen: &commonpb.Timestamp{Data: 1000000},
			lastSeen:  &commonpb.Timestamp{Data: 2000000},
			want:      `"firstSeen":"1970-01-01T00:00:01Z","lastSeen":"1970-01-01T00:00:02Z"`,
		},
		{
			name: "absent",
			want: `"temporal":{"transactionsPerDay":1}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pattern := &servicepb.FlowPattern{
				TransactionCount: 1,
				Temporal: &servicepb.TemporalStats{
					FirstSeen:          tc.firstSeen,
					LastSeen:           tc.lastSeen,
					TransactionsPerDay: 1,
				},
			}

			converted, err := toFlowPatternJSON(pattern)
			require.NoError(t, err)
			encoded, err := stdjson.Marshal(converted)
			require.NoError(t, err)
			require.Contains(t, string(encoded), tc.want)

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AnalyzeTransactions(gomock.Any(), "my-ledger", uint32(0), gomock.Any()).Return(&servicepb.AnalyzeTransactionsResponse{
				TotalTransactions: 1,
				FlowPatterns:      []*servicepb.FlowPattern{pattern},
			}, nil)
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v3/my-ledger/analyze-transactions", nil))
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), tc.want)

			wrapper := decodeResponse[BaseResponse[struct {
				FlowPatterns []struct {
					Temporal struct {
						FirstSeen *string `json:"firstSeen"`
						LastSeen  *string `json:"lastSeen"`
					} `json:"temporal"`
				} `json:"flowPatterns"`
			}]](t, w)
			require.Len(t, wrapper.Data.FlowPatterns, 1)
			temporal := wrapper.Data.FlowPatterns[0].Temporal
			for _, field := range []struct {
				source  *commonpb.Timestamp
				decoded *string
			}{{tc.firstSeen, temporal.FirstSeen}, {tc.lastSeen, temporal.LastSeen}} {
				if field.source == nil {
					require.Nil(t, field.decoded)

					continue
				}
				require.NotNil(t, field.decoded)
				decoded, err := stdtime.Parse(stdtime.RFC3339Nano, *field.decoded)
				require.NoError(t, err)
				require.Equal(t, int64(field.source.GetData()), decoded.UnixMicro())
			}
		})
	}
}
