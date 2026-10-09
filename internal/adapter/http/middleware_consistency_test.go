package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// serveGetAccount sends GET /v3/ledger1/accounts/alice through the real router
// with the given X-Consistency values and returns the recorder plus the
// consistency level the backend observed ("" when the backend was not called).
func serveGetAccount(t *testing.T, headerValues ...string) (*httptest.ResponseRecorder, string) {
	t.Helper()

	var observed string

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).
		Return(&commonpb.LedgerInfo{Name: "ledger1"}, nil).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), "ledger1", "alice", gomock.Any()).
		DoAndReturn(func(ctx context.Context, _, addr string, _ ctrl.GetAccountOptions) (*commonpb.Account, error) {
			observed = query.ConsistencyFromContext(ctx)

			return &commonpb.Account{Address: addr}, nil
		}).AnyTimes()

	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	r := httptest.NewRequest(http.MethodGet, APIVersionPrefix+"/ledger1/accounts/alice", nil)
	for _, v := range headerValues {
		r.Header.Add(headerConsistency, v)
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	return w, observed
}

func TestReadConsistency_SelectsLevel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		header []string
		want   string
	}{
		{name: "absent defaults to linearizable", want: query.ConsistencyLinearizable},
		{name: "stale", header: []string{"stale"}, want: query.ConsistencyStale},
		{name: "stale is case and space insensitive", header: []string{" Stale "}, want: query.ConsistencyStale},
		{name: "explicit linearizable", header: []string{"linearizable"}, want: query.ConsistencyLinearizable},
		{name: "empty value is the default", header: []string{""}, want: query.ConsistencyLinearizable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w, observed := serveGetAccount(t, tc.header...)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, tc.want, observed)
		})
	}
}

func TestReadConsistency_RejectsUnsupportedSelector(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		header  []string
		message string
	}{
		// EN-1946 removed `leader`; HTTP must not accept it as an alias.
		{name: "removed leader selector", header: []string{"leader"}, message: `unsupported X-Consistency "leader"`},
		{name: "unknown value", header: []string{"eventual"}, message: `unsupported X-Consistency "eventual"`},
		{name: "repeated header", header: []string{"stale", "stale"}, message: "X-Consistency must be set at most once"},
		{name: "comma-joined repeat", header: []string{"stale, stale"}, message: "X-Consistency must be set at most once"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w, observed := serveGetAccount(t, tc.header...)

			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Equal(t, "application/json", w.Header().Get("Content-Type"))
			require.Empty(t, observed, "a rejected selector must not reach the backend")

			var body ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, "INVALID_REQUEST", body.ErrorCode)
			require.Contains(t, body.ErrorMessage, tc.message)
		})
	}
}

// Every business route must see the selector, otherwise a caller asking for a
// stale read on that route silently gets a linearizable one (or the reverse
// contract drifts per route). Ops routes are unversioned and read no ledger.
func TestReadConsistency_InstalledOnEveryBusinessRoute(t *testing.T) {
	t.Parallel()

	chains := walkRouteMiddlewares(t)
	checked := 0

	for key, chain := range chains {
		_, pattern, _ := strings.Cut(key, " ")
		if !strings.HasPrefix(pattern, APIVersionPrefix+"/") {
			assert.Equal(t, -1, indexOfMiddleware(chain, "readConsistency"),
				"ops route %s must not parse X-Consistency", key)

			continue
		}

		checked++
		assert.GreaterOrEqual(t, indexOfMiddleware(chain, "readConsistency"), 0,
			"business route %s does not honour X-Consistency", key)
	}

	require.Positive(t, checked, "the walk found no business routes")
}
