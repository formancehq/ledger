package v1

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

// STRIX-32 / 29a87ddc-2677-41dd-b904-db140254e3de — corrected regression
// contract at the v1 HTTP trust boundary.
//
// On the v1 list route, expand arrives from r.URL.Query()["expand"]
// (v1 getResourceQuery) or from the unsigned base64 cursor, and is forwarded
// verbatim into storagecommon.ResourceQuery.Expand. The storage layer is the
// enforcement point: unsupported expand values must be rejected there with
// storagecommon.ErrInvalidQuery.
//
// SAFE invariant asserted here: when the storage layer rejects an unsupported
// expand value with ErrInvalidQuery, the v1 list route must answer an error
// status (>= 400), never 200 with data. The v1 list handler maps unknown
// errors through HandleCommonErrors, which has no ErrInvalidQuery case, so a
// rejected expand currently surfaces as 500 INTERNAL — a boundary limit to
// record, not a security leak: no SQL built from the payload is executed and
// no data is rendered. The assertions below accept any error status, so they
// stay green on a correct fix even if the v1 mapping is later improved to
// 400 VALIDATION.

const strix32Payload = `x,/**/(select/**/pg_sleep(2))as/**/y`

// TestSTRIX32V1ListRejectsUnsupportedExpand: v1 list with an unsupported
// expand query parameter must not return 200 with data.
func TestSTRIX32V1ListRejectsUnsupportedExpand(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		ListAccounts(gomock.Any(), storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: DefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{strix32Payload},
			},
		}).
		Return(nil, storagecommon.NewErrInvalidQuery("invalid expand property %s", strix32Payload))

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

	params := url.Values{}
	params.Set("expand", strix32Payload)
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.NotEqual(t, http.StatusOK, rec.Code, "unsupported expand must never yield a successful response")
	require.GreaterOrEqual(t, rec.Code, 400)
}

// TestSTRIX32V1TamperedCursorCarriesExpand: the v1 list route accepts a
// base64 cursor whose embedded "filters.expand" is attacker-controlled (no
// signature). SAFE contract: when that expand value is rejected with
// ErrInvalidQuery, the route must answer an error status, never 200 with data.
func TestSTRIX32V1TamperedCursorCarriesExpand(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(storagecommon.OffsetPaginatedQuery[any]{
		InitialPaginatedQuery: storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: DefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{strix32Payload},
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
				PageSize: DefaultPageSize,
				Options: storagecommon.ResourceQuery[any]{
					Expand: []string{strix32Payload},
				},
			},
		}).
		Return(nil, storagecommon.NewErrInvalidQuery("invalid expand property %s", strix32Payload))

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

	params := url.Values{}
	params.Set("cursor", tampered)
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.NotEqual(t, http.StatusOK, rec.Code, "tampered-cursor expand must never yield a successful response")
	require.GreaterOrEqual(t, rec.Code, 400)
}

// TestSTRIX32V1ListLegitimateExpandAccepted: legitimate expand values keep
// flowing to the storage layer and a successful storage answer renders 200 —
// pins that the corrected contract does not break supported expands on v1.
func TestSTRIX32V1ListLegitimateExpandAccepted(t *testing.T) {
	t.Parallel()

	systemController, ledgerController := newTestingSystemController(t, true)
	ledgerController.EXPECT().
		ListAccounts(gomock.Any(), storagecommon.InitialPaginatedQuery[any]{
			Column:   "address",
			Order:    pointer.For(paginate.Order(paginate.OrderAsc)),
			PageSize: DefaultPageSize,
			Options: storagecommon.ResourceQuery[any]{
				Expand: []string{"volumes"},
			},
		}).
		Return(&paginate.Cursor[ledger.Account]{
			Data: []ledger.Account{{Address: "world", Metadata: metadata.Metadata{}}},
		}, nil)

	router := NewRouter(systemController, jwt.NewNoAuth(), "develop", os.Getenv("DEBUG") == "true")

	params := url.Values{}
	params.Set("expand", "volumes")
	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
	req.URL.RawQuery = params.Encode()
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}
