package v2

import (
	"bytes"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/auth"
	"github.com/formancehq/go-libs/v4/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/queries"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

func TestQueriesRun(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	router := NewRouter(systemController, auth.NewNoAuth(), "develop")

	expectedResourceKind := queries.ResourceKindTransaction
	expectedCursor := bunpaginate.Cursor[any]{
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
				"foo": float64(123.0),
				"bar": "barnacle",
			},
		}, storagecommon.PaginationConfig{
			MaxPageSize:     bunpaginate.MaxPageSize,
			DefaultPageSize: bunpaginate.QueryDefaultPageSize,
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
