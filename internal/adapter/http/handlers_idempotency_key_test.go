package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHTTPMutationsPreserveIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, path, body string
		params                   map[string]string
		handle                   func(*Server, http.ResponseWriter, *http.Request)
	}{
		{"add account type", http.MethodPost, "/ledger1/account-types", `{"name":"users","pattern":"users:*"}`, map[string]string{"ledgerName": "ledger1"}, (*Server).handleAddAccountType},
		{"remove account type", http.MethodDelete, "/ledger1/account-types/users", "", map[string]string{"ledgerName": "ledger1", "typeName": "users"}, (*Server).handleRemoveAccountType},
		{"set metadata type", http.MethodPut, "/ledger1/metadata-schema/account/age", `{"type":"int64"}`, map[string]string{"ledgerName": "ledger1", "targetType": "account", "key": "age"}, (*Server).handleSetMetadataType},
		{"remove metadata type", http.MethodDelete, "/ledger1/metadata-schema/account/age", "", map[string]string{"ledgerName": "ledger1", "targetType": "account", "key": "age"}, (*Server).handleRemoveMetadataType},
		{"set default enforcement mode", http.MethodPut, "/ledger1/account-types/default-enforcement-mode", `{"enforcementMode":"STRICT"}`, map[string]string{"ledgerName": "ledger1"}, (*Server).handleSetDefaultEnforcementMode},
		{"update prepared query", http.MethodPut, "/ledger1/prepared-queries/my-query", `{"target":"TRANSACTIONS","filter":` + validFieldFilterJSON + `}`, map[string]string{"ledgerName": "ledger1", "queryName": "my-query"}, (*Server).handleUpdatePreparedQuery},
		{"clear prepared query filter", http.MethodPut, "/ledger1/prepared-queries/my-query", `{"target":"TRANSACTIONS","filter":null}`, map[string]string{"ledgerName": "ledger1", "queryName": "my-query"}, (*Server).handleUpdatePreparedQuery},
		{"delete prepared query", http.MethodDelete, "/ledger1/prepared-queries/my-query", "", map[string]string{"ledgerName": "ledger1", "queryName": "my-query"}, (*Server).handleDeletePreparedQuery},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
					require.Equal(t, "en-3286-key", req.GetUnsigned().GetIdempotencyKey())

					return &domain.ApplyResult{Logs: []*commonpb.Log{{}}}, nil
				})
			srv := newTestServer(t, backend)
			r := newRequest(t, tc.method, tc.path, strings.NewReader(tc.body), tc.params)
			r.Header.Set("Idempotency-Key", "en-3286-key")
			w := httptest.NewRecorder()
			tc.handle(srv, w, r)
			require.Less(t, w.Code, 300, w.Body.String())
		})
	}
}

func TestUpdatePreparedQueryOmittedFilterDoesNotConsumeKey(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	// Current HTTP contract rejects omitted filters before Apply (#2616).
	srv := newTestServer(t, backend)
	r := newRequest(t, http.MethodPut, "/ledger1/prepared-queries/my-query",
		strings.NewReader(`{"target":"TRANSACTIONS"}`), map[string]string{"ledgerName": "ledger1", "queryName": "my-query"})
	r.Header.Set("Idempotency-Key", "en-3286-no-op")
	w := httptest.NewRecorder()
	srv.handleUpdatePreparedQuery(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
