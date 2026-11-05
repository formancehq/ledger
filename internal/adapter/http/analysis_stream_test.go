package http

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestAnalysisStream_Accounts(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeAccounts(gomock.Any(), "books", uint32(25), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, progress func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			require.NotNil(t, progress)
			if progress != nil {
				progress(500, 0)
			}

			return &servicepb.AnalyzeAccountsResponse{TotalAccounts: 501}, nil
		})
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
	r := httptest.NewRequest(http.MethodGet, "/v3/books/analyze-accounts?variableThreshold=25", nil)
	r.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	require.True(t, w.Flushed)
	require.Equal(t, "event: started\ndata: {}\n\nevent: progress\ndata: {\"processed\":500,\"total\":0,\"phase\":\"scanning\"}\n\nevent: result\ndata: {\"data\":{\"patterns\":[],\"totalAccounts\":501}}\n\n", w.Body.String())
}

// exerciseAnalysisRoute uses the registered router and its generated boundary
// mock. Each request may invoke exactly one existing analysis operation.
func exerciseAnalysisRoute(t *testing.T, route string, run func(context.Context, func(uint64, uint64)) error) http.Handler {
	t.Helper()
	backend := NewMockBackend(gomock.NewController(t))
	if route == "accounts" {
		backend.EXPECT().AnalyzeAccounts(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ string, _ uint32, progress func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
				err := run(ctx, progress)

				return &servicepb.AnalyzeAccountsResponse{}, err
			})
	} else {
		backend.EXPECT().AnalyzeTransactions(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ string, _ uint32, progress func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
				err := run(ctx, progress)

				return &servicepb.AnalyzeTransactionsResponse{}, err
			})
	}

	return NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
}

func analysisRequest(route, accept string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v3/books/analyze-"+route, nil)
	r.Header.Set("Accept", accept)

	return r
}

func TestAnalysisStream_Negotiation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		accept string
		stream bool
	}{
		{"", false}, {"*/*", false}, {"text/*", false}, {"application/json", false},
		{"text/event-stream", true}, {"Text/Event-Stream; charset=utf-8", true},
		{"text/event-stream;q=0", false},
		{"application/xml;q=1,text/event-stream;q=0.5", true}, {"text/event-stream;q=0.5, application/json;q=1", false},
		{"application/json;q=0.5, text/event-stream;q=0.8", true},
		{"text/event-stream;q=0.5, */*;q=1", false},
		{"text/event-stream;q=0.5, application/json;q=0, */*;q=1", true},
		{"text/event-stream;q=NaN", false}, {"text/event-stream;q=2", false},
		{"text/event-stream;q=no", false}, {"text/event-stream;q=0.5, application/json;q=0.5", true},
	} {
		t.Run(tc.accept, func(t *testing.T) {
			t.Parallel()
			handler := exerciseAnalysisRoute(t, "accounts", func(_ context.Context, progress func(uint64, uint64)) error {
				require.Equal(t, tc.stream, progress != nil)

				return nil
			})
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, analysisRequest("accounts", tc.accept))
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Header().Values("Vary"), "Accept")
			if tc.stream {
				require.Contains(t, w.Body.String(), "event: result\n")
			} else {
				require.Equal(t, "application/json", w.Header().Get("Content-Type"))
				require.JSONEq(t, `{"data":{"patterns":[],"totalAccounts":0}}`, w.Body.String())
			}
		})
	}
}

func TestAnalysisStream_TerminalEvents(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		for _, tc := range []struct {
			name       string
			err        error
			panicValue any
			code       string
			status     int
			retry      int
		}{
			{name: "empty"},
			{name: "notFound", err: commonpb.NewNotFoundError("missing"), code: "NOT_FOUND", status: 404},
			{name: "unavailable", err: commonpb.ErrNoLeader, code: "NO_LEADER", status: 503, retry: 1},
			{name: "internal", err: errors.New("secret /store/path"), code: "INTERNAL_ERROR", status: 500},
			{name: "panic", panicValue: "secret panic /store/path", code: "INTERNAL_ERROR", status: 500},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				handler := exerciseAnalysisRoute(t, route, func(_ context.Context, progress func(uint64, uint64)) error {
					require.NotNil(t, progress)
					if tc.name != "empty" {
						progress(500, 0)
					}
					if tc.panicValue != nil {
						panic(tc.panicValue)
					}

					return tc.err
				})
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, analysisRequest(route, "text/event-stream"))
				require.Equal(t, http.StatusOK, w.Code)
				require.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
				require.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
				require.Equal(t, "no", w.Header().Get("X-Accel-Buffering"))
				frames := strings.Split(strings.TrimSuffix(w.Body.String(), "\n\n"), "\n\n")
				require.Equal(t, "event: started\ndata: {}", frames[0])
				if tc.name == "empty" {
					require.Len(t, frames, 2)
					require.True(t, strings.HasPrefix(frames[1], "event: result\ndata: "))

					return
				}
				require.Len(t, frames, 3)
				phase := ""
				if route == "accounts" {
					phase = "scanning"
				}
				require.Equal(t, fmt.Sprintf("event: progress\ndata: {\"processed\":500,\"total\":0,\"phase\":%q}", phase), frames[1])
				require.True(t, strings.HasPrefix(frames[2], "event: error\ndata: "))
				var body analysisErrorJSON
				require.NoError(t, stdjson.Unmarshal([]byte(strings.TrimPrefix(frames[2], "event: error\ndata: ")), &body))
				require.Equal(t, tc.code, body.ErrorCode)
				require.Equal(t, tc.status, body.Status)
				require.Equal(t, tc.retry, body.RetryAfter)
				require.NotContains(t, w.Body.String(), "secret")
				require.NotContains(t, w.Body.String(), "/store/path")
				require.NotContains(t, w.Body.String(), "event: result")
			})
		}
	}
}

func TestAnalysisStream_CanceledRequestNeverCompletes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handler := exerciseAnalysisRoute(t, route, func(ctx context.Context, progress func(uint64, uint64)) error {
				progress(500, 0)
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				progress(501, 0) // even a backend returning success after cancellation cannot emit a result

				return nil
			})
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, analysisRequest(route, "text/event-stream").WithContext(ctx))
			require.Contains(t, w.Body.String(), "event: progress")
			require.NotContains(t, w.Body.String(), "event: result")
			require.NotContains(t, w.Body.String(), "event: error")
		})
	}
}

func TestAnalysisStream_TransactionsBigIntegersAndProjectionFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, volume, header, want string
		failure                    bool
	}{
		{name: "number", volume: "9007199254740993", want: `"totalVolume":9007199254740993`},
		{name: "string", volume: "9007199254740993", header: "true", want: `"totalVolume":"9007199254740993"`},
		{name: "invalid", volume: "invalid secret", failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().AnalyzeTransactions(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string, _ uint32, progress func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
					progress(1001, 1002)

					return &servicepb.AnalyzeTransactionsResponse{TotalTransactions: 501, FlowPatterns: []*servicepb.FlowPattern{{VolumeStats: []*servicepb.AssetVolumeStats{{Asset: "USD", TotalVolume: tc.volume, AverageVolume: "1", MinVolume: "1", MaxVolume: tc.volume}}}}}, nil
				})
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := analysisRequest("transactions", "text/event-stream")
			r.Header.Set("Formance-Bigint-As-String", tc.header)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Contains(t, w.Body.String(), `"processed":1001,"total":1002,"phase":""`)
			if tc.failure {
				require.Contains(t, w.Body.String(), "event: error\n")
				require.Contains(t, w.Body.String(), `"status":500`)
				require.NotContains(t, w.Body.String(), "secret")
				require.NotContains(t, w.Body.String(), "event: result")
			} else {
				require.Contains(t, w.Body.String(), "event: result\n")
				require.Contains(t, w.Body.String(), tc.want)
				require.Contains(t, w.Body.String(), `"totalTransactions":501`)
			}
		})
	}
}

func TestAnalysisStream_EarlyValidationAndAuthorization(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t)) // no backend calls allowed
			for _, suffix := range []string{"?variableThreshold=bad", "?unknown=true"} {
				r := analysisRequest(route, "text/event-stream")
				r.URL.RawQuery = strings.TrimPrefix(suffix, "?")
				w := httptest.NewRecorder()
				NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(w, r)
				require.Equal(t, http.StatusBadRequest, w.Code)
				require.NotContains(t, w.Body.String(), "event:")
			}
			key, keySet := testKeyPair(t)
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuer: testAuthIssuer, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{})
			for _, token := range []string{"", signToken(t, key, testClaims(string(internalauth.ScopeTransactionsRead)))} {
				r := analysisRequest(route, "text/event-stream")
				if token != "" {
					r.Header.Set("Authorization", "Bearer "+token)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if token == "" {
					require.Equal(t, http.StatusUnauthorized, w.Code)
				} else {
					require.Equal(t, http.StatusForbidden, w.Code)
				}
				require.NotContains(t, w.Body.String(), "event:")
			}
		})
	}
}

func TestAnalysisStream_EncodingFailureIsTerminal(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().AnalyzeTransactions(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, progress func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			progress(500, 0)

			return &servicepb.AnalyzeTransactionsResponse{FlowPatterns: []*servicepb.FlowPattern{{Temporal: &servicepb.TemporalStats{TransactionsPerDay: math.NaN()}}}}, nil
		})
	w := httptest.NewRecorder()
	NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(w, analysisRequest("transactions", "text/event-stream"))
	require.Contains(t, w.Body.String(), "event: error\n")
	require.Contains(t, w.Body.String(), `"errorCode":"INTERNAL_ERROR"`)
	require.NotContains(t, w.Body.String(), "event: result")
	require.NotContains(t, w.Body.String(), "NaN")
}

func TestAnalysisStream_AuthorizedConsistency(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			key, keySet := testKeyPair(t)
			backend := NewMockBackend(gomock.NewController(t))
			run := func(ctx context.Context) {
				require.Equal(t, query.ConsistencyStale, query.ConsistencyFromContext(ctx))
				require.NoError(t, ctx.Err())
			}
			if route == "accounts" {
				backend.EXPECT().AnalyzeAccounts(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
					run(ctx)

					return &servicepb.AnalyzeAccountsResponse{}, nil
				})
			} else {
				backend.EXPECT().AnalyzeTransactions(gomock.Any(), "books", uint32(0), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ uint32, _ func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
					run(ctx)

					return &servicepb.AnalyzeTransactionsResponse{}, nil
				})
			}
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuer: testAuthIssuer, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{})
			r := analysisRequest(route, "text/event-stream")
			r.Header.Set("Authorization", "Bearer "+signToken(t, key, testClaims(string(internalauth.ScopeAccountsRead))))
			r.Header.Set("X-Consistency", "stale")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), "event: result\n")
		})
	}
}
