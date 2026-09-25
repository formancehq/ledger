package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/queries"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func TestQueriesRun(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	expectedResourceKind := queries.ResourceKindTransaction
	expectedCursor := paginate.Cursor[any]{
		Data: []any{
			ledger.NewTransaction().WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
		},
	}

	expectedResponse, err := json.Marshal(map[string]any{
		"resource": "transactions",
		"cursor": map[string]any{
			"data": []map[string]any{
				{
					"id":         nil,
					"insertedAt": "0001-01-01T00:00:00Z",
					"metadata":   map[string]any{},
					"postings": []map[string]any{
						{"amount": 100, "asset": "USD", "destination": "bank", "source": "world"},
					},
					"reverted":  false,
					"timestamp": "0001-01-01T00:00:00Z",
					"updatedAt": "0001-01-01T00:00:00Z",
				},
			},
			"hasMore": false,
		},
	})
	require.NoError(t, err)

	ledgerController.EXPECT().
		RunQuery(gomock.Any(), "1.2.3", "QUERY_ID", storagecommon.RunQuery{
			Params: json.RawMessage(`{ "pageSize": 42 }`),
			Vars: map[string]any{
				"foo": json.Number("123"),
				"bar": "barnacle",
			},
		}, storagecommon.PaginationConfig{
			MaxPageSize:     paginate.MaxPageSize,
			DefaultPageSize: paginate.QueryDefaultPageSize,
		}).
		Return(&expectedResourceKind, &expectedCursor, nil)

	req := httptest.NewRequest(http.MethodPost, "/xxx/queries/QUERY_ID/run?schemaVersion=1.2.3", bytes.NewBufferString(`{
		"params": { "pageSize": 42 },
		"vars": {
			"foo": 123,
			"bar": "barnacle"
		}
	}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	b := []byte{}
	_, _ = rec.Body.Read(b)
	spew.Dump(string(b))
	require.Equal(t, http.StatusOK, rec.Code)

	require.JSONEq(t, rec.Body.String(), string(expectedResponse))
}

func TestQueriesRunPreservesLargeIntegerVars(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	expectedResourceKind := queries.ResourceKindAccount
	var received storagecommon.RunQuery
	ledgerController.EXPECT().
		RunQuery(gomock.Any(), "1.2.3", "QUERY_ID", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ string, q storagecommon.RunQuery, _ storagecommon.PaginationConfig) (*queries.ResourceKind, *paginate.Cursor[any], error) {
			received = q
			return &expectedResourceKind, &paginate.Cursor[any]{Data: []any{}}, nil
		})

	// 2^53 + 1 cannot be represented exactly as a float64.
	req := httptest.NewRequest(http.MethodPost, "/xxx/queries/QUERY_ID/run?schemaVersion=1.2.3", bytes.NewBufferString(`{
		"vars": {"minimum_balance": 9007199254740993}
	}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Resolve the variable the same way the ledger controller does.
	builder, err := queries.ResolveFilterTemplate(
		queries.ResourceKindAccount,
		json.RawMessage(`{"$gte": {"balance[COIN]": "${minimum_balance}"}}`),
		map[string]queries.VarDecl{"minimum_balance": {Type: queries.NewTypeNumeric()}},
		received.Vars,
	)
	require.NoError(t, err)

	var resolved any
	require.NoError(t, builder.Walk(func(_ string, _ string, value *any) error {
		resolved = *value
		return nil
	}))
	expected, _ := new(big.Int).SetString("9007199254740993", 10)
	require.Equal(t, expected.String(), resolved.(*big.Int).String())
}
