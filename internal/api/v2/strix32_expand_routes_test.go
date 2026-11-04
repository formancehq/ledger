package v2

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

// STRIX-32 / 29a87ddc-2677-41dd-b904-db140254e3de — corrected regression
// contract at the v2 HTTP trust boundary.
//
// The expand query parameter is forwarded into
// storagecommon.ResourceQuery.Expand on every accounts route (list, read)
// and inside the base64 cursor payload, with no allow-list at the HTTP
// boundary. The storage layer is the enforcement point: unsupported expand
// values must be rejected there with storagecommon.ErrInvalidQuery.
//
// These tests pin the boundary behavior with the SAFE invariant: when the
// storage layer rejects an unsupported expand value with ErrInvalidQuery, the
// v2 list route answers 400 VALIDATION (HandleCommonPaginationErrors) and the
// v2 read route answers an error status, never 200 with data
// (HandleCommonErrors has no ErrInvalidQuery case, so the read route surfaces
// it as 500 INTERNAL — a boundary limit to record, not a security leak: the
// error path never executes SQL and never renders data). The mocks below
// return exactly the error a correct storage layer returns; the assertions
// encode only that safe outcome, so they pass on a fixed candidate and fail
// on the vulnerable base where no such error exists.

// strix32InjectionExpand is an expand value that the storage layer must
// reject as invalid (on the vulnerable base it is interpolated into SQL).
const strix32InjectionExpand = `x,/**/(select/**/pg_sleep(2))as/**/y`

// strix32ExpandParam is comma-free because v2 getExpand splits each
// "expand" query value on commas.
const strix32ExpandParam = `x/**/(select/**/pg_sleep(2))as/**/y`

// TestSTRIX32V2ListRejectsUnsupportedExpand: v2 list with an unsupported
// expand value must surface the storage ErrInvalidQuery as 400 VALIDATION.
func TestSTRIX32V2ListRejectsUnsupportedExpand(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		ListAccounts(gomock.Any(), storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: paginate.QueryDefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{strix32ExpandParam},
			},
		}).
		Return(nil, storagecommon.NewErrInvalidQuery("invalid expand property %s", strix32ExpandParam))

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	params := url.Values{}
	params.Set("expand", strix32ExpandParam)
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	errResp := api.ErrorResponse{}
	api.Decode(t, rec.Body, &errResp)
	require.EqualValues(t, common.ErrValidation, errResp.ErrorCode)
}

// TestSTRIX32V2ReadRejectsUnsupportedExpand: v2 read with an unsupported
// expand value must not return 200 with data; the storage ErrInvalidQuery
// must surface as an error response (never a successful account payload).
func TestSTRIX32V2ReadRejectsUnsupportedExpand(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		GetAccount(gomock.Any(), storagecommon.ResourceQuery[any]{
			Builder: query.Match("address", "users:alice"),
			Expand:  []string{strix32InjectionExpand},
		}).
		Return(nil, storagecommon.NewErrInvalidQuery("invalid expand property %s", strix32InjectionExpand))

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	params := url.Values{}
	params.Set("expand", strix32InjectionExpand)
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts/users:alice", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.NotEqual(t, http.StatusOK, rec.Code, "unsupported expand must never yield a successful response")
	require.GreaterOrEqual(t, rec.Code, 400)
}

// TestSTRIX32V2TamperedCursorCarriesExpand: a base64 cursor embeds
// "filters.expand" with no signature; UnmarshalCursor decodes it verbatim.
// SAFE contract: when that expand value is rejected with ErrInvalidQuery, the
// list route must answer 400 VALIDATION — not 200 with data.
func TestSTRIX32V2TamperedCursorCarriesExpand(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(storagecommon.OffsetPaginatedQuery[any]{
		InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: paginate.QueryDefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{strix32InjectionExpand},
			},
		},
	})
	require.NoError(t, err)
	tampered := base64.RawURLEncoding.EncodeToString(raw)

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		ListAccounts(gomock.Any(), storagecommon.OffsetPaginatedQuery[any]{
			InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
				Column:   "address",
				Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
				PageSize: paginate.QueryDefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					Expand: []string{strix32InjectionExpand},
				},
			},
		}).
		Return(nil, storagecommon.NewErrInvalidQuery("invalid expand property %s", strix32InjectionExpand))

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	params := url.Values{}
	params.Set("cursor", tampered)
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	errResp := api.ErrorResponse{}
	api.Decode(t, rec.Body, &errResp)
	require.EqualValues(t, common.ErrValidation, errResp.ErrorCode)
}

// TestSTRIX32V2ListLegitimateExpandAccepted: legitimate expand values keep
// flowing to the storage layer and a successful storage answer renders 200 —
// pins that the corrected contract does not break supported expands.
func TestSTRIX32V2ListLegitimateExpandAccepted(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		ListAccounts(gomock.Any(), storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: paginate.QueryDefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{"volumes"},
			},
		}).
		Return(&paginate.Cursor[ledger.Account]{
			Data: []ledger.Account{{Address: "world", Metadata: metadata.Metadata{}}},
		}, nil)

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	params := url.Values{}
	params.Set("expand", "volumes")
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
