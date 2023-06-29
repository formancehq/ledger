package controllers_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestGetAccounts(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectQuery       ledgerstore.AccountsQuery
		expectStatusCode  int
		expectedErrorCode string
	}

	testCases := []testCase{
		{
			name:        "nominal",
			expectQuery: ledgerstore.NewAccountsQuery(),
		},
		{
			name: "using metadata",
			queryParams: url.Values{
				"metadata[roles]": []string{"admin"},
			},
			expectQuery: ledgerstore.NewAccountsQuery().
				WithMetadataFilter(map[string]string{
					"roles": "admin",
				}),
		},
		{
			name: "using nested metadata",
			queryParams: url.Values{
				"metadata[a.nested.key]": []string{"hello"},
			},
			expectQuery: ledgerstore.NewAccountsQuery().
				WithMetadataFilter(map[string]string{
					"a.nested.key": "hello",
				}),
		},
		{
			name: "using after",
			queryParams: url.Values{
				"after": []string{"foo"},
			},
			expectQuery: ledgerstore.NewAccountsQuery().
				WithAfterAddress("foo").
				WithMetadataFilter(map[string]string{}),
		},
		{
			name: "using address",
			queryParams: url.Values{
				"address": []string{"foo"},
			},
			expectQuery: ledgerstore.NewAccountsQuery().
				WithAddressFilter("foo").
				WithMetadataFilter(map[string]string{}),
		},
		{
			name: "using empty cursor",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.EncodeCursor(ledgerstore.NewAccountsQuery())},
			},
			expectQuery: ledgerstore.NewAccountsQuery(),
		},
		{
			name: "using cursor with other param",
			queryParams: url.Values{
				"cursor": []string{ledgerstore.EncodeCursor(ledgerstore.NewAccountsQuery())},
				"after":  []string{"foo"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "using invalid cursor",
			queryParams: url.Values{
				"cursor": []string{"XXX"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "invalid page size",
			queryParams: url.Values{
				"pageSize": []string{"nan"},
			},
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name: "page size over maximum",
			queryParams: url.Values{
				"pageSize": []string{"1000000"},
			},
			expectQuery: ledgerstore.NewAccountsQuery().
				WithPageSize(controllers.MaxPageSize).
				WithMetadataFilter(map[string]string{}),
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusOK
			}

			expectedCursor := sharedapi.Cursor[core.Account]{
				Data: []core.Account{
					{
						Address:  "world",
						Metadata: metadata.Metadata{},
					},
				},
			}

			backend, mockLedger := newTestingBackend(t)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				mockLedger.EXPECT().
					GetAccounts(gomock.Any(), testCase.expectQuery).
					Return(&expectedCursor, nil)
			}

			router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodGet, "/xxx/accounts", nil)
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode < 300 && testCase.expectStatusCode >= 200 {
				cursor := sharedapi.DecodeCursorResponse[core.Account](t, rec.Body)
				require.Equal(t, expectedCursor, *cursor)
			} else {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}

func TestGetAccount(t *testing.T) {
	t.Parallel()

	account := core.AccountWithVolumes{
		Account: core.Account{
			Address:  "foo",
			Metadata: metadata.Metadata{},
		},
		Volumes: core.VolumesByAssets{},
	}

	backend, mock := newTestingBackend(t)
	mock.EXPECT().
		GetAccount(gomock.Any(), "foo").
		Return(&account, nil)

	router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

	req := httptest.NewRequest(http.MethodGet, "/xxx/accounts/foo", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	response, _ := sharedapi.DecodeSingleResponse[core.AccountWithVolumes](t, rec.Body)
	require.Equal(t, account, response)
}

func TestPostAccountMetadata(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		queryParams       url.Values
		expectStatusCode  int
		expectedErrorCode string
		account           string
		body              any
	}

	testCases := []testCase{
		{
			name:    "nominal",
			account: "world",
			body: metadata.Metadata{
				"foo": "bar",
			},
		},
		{
			name:              "invalid account address format",
			account:           "invalid-acc",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
		{
			name:              "invalid body",
			account:           "world",
			body:              "invalid - not an object",
			expectStatusCode:  http.StatusBadRequest,
			expectedErrorCode: apierrors.ErrValidation,
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {

			if testCase.expectStatusCode == 0 {
				testCase.expectStatusCode = http.StatusNoContent
			}

			backend, mock := newTestingBackend(t)
			if testCase.expectStatusCode == http.StatusNoContent {
				mock.EXPECT().
					SaveMeta(gomock.Any(), command.Parameters{}, core.MetaTargetTypeAccount, testCase.account, testCase.body).
					Return(nil)
			}

			router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())

			req := httptest.NewRequest(http.MethodPost, "/xxx/accounts/"+testCase.account+"/metadata", sharedapi.Buffer(t, testCase.body))
			rec := httptest.NewRecorder()
			req.URL.RawQuery = testCase.queryParams.Encode()

			router.ServeHTTP(rec, req)

			require.Equal(t, testCase.expectStatusCode, rec.Code)
			if testCase.expectStatusCode >= 300 || testCase.expectStatusCode < 200 {
				err := sharedapi.ErrorResponse{}
				sharedapi.Decode(t, rec.Body, &err)
				require.EqualValues(t, testCase.expectedErrorCode, err.ErrorCode)
			}
		})
	}
}
