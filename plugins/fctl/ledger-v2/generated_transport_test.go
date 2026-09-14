package ledgerv2

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
)

type captureHTTPClient struct {
	request *http.Request
	body    []byte
}

func (c *captureHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.request = request
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		c.body = body
	}
	return &http.Response{StatusCode: 204, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func TestGeneratedTransportRemovesOnlyByteExactNullGETBody(t *testing.T) {
	capture := &captureHTTPClient{}
	transport := stripGeneratedNullGETBody{next: capture}
	request, err := http.NewRequest(http.MethodGet, "https://product.invalid/v2?cursor=opaque", bytes.NewBufferString("null"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if _, err := transport.Do(request); err != nil {
		t.Fatal(err)
	}
	if len(capture.body) != 0 || capture.request.Body != http.NoBody || capture.request.ContentLength != 0 || capture.request.Header.Get("Content-Type") != "" {
		t.Fatalf("forwarded request = body %q content-length %d content-type %q", capture.body, capture.request.ContentLength, capture.request.Header.Get("Content-Type"))
	}
}

func TestGeneratedTransportPreservesEveryOtherBody(t *testing.T) {
	for _, body := range []string{`{"filter":"value"}`, `null `, `NULL`} {
		t.Run(body, func(t *testing.T) {
			capture := &captureHTTPClient{}
			transport := stripGeneratedNullGETBody{next: capture}
			request, err := http.NewRequest(http.MethodGet, "https://product.invalid/v2", bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			if _, err := transport.Do(request); err != nil {
				t.Fatal(err)
			}
			if string(capture.body) != body || capture.request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("forwarded body = %q content-type %q", capture.body, capture.request.Header.Get("Content-Type"))
			}
		})
	}
}

func TestGeneratedTransportMakesNonEmptyCursorQueriesExclusive(t *testing.T) {
	tests := []struct {
		name, rawURL, wantQuery string
	}{
		{name: "generated default removed", rawURL: "https://product.invalid/v2?cursor=opaque&includeDeleted=false&pageSize=15", wantQuery: "cursor=opaque"},
		{name: "cursor canonically encoded", rawURL: "https://product.invalid/v2?cursor=opaque%20%2F%3F%26&sort=id%3Adesc", wantQuery: "cursor=opaque+%2F%3F%26"},
		{name: "empty cursor untouched", rawURL: "https://product.invalid/v2?cursor=&includeDeleted=false", wantQuery: "cursor=&includeDeleted=false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := &captureHTTPClient{}
			transport := stripGeneratedNullGETBody{next: capture}
			request, err := http.NewRequest(http.MethodGet, test.rawURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := transport.Do(request); err != nil {
				t.Fatal(err)
			}
			if got := capture.request.URL.RawQuery; got != test.wantQuery {
				t.Fatalf("query = %q, want %q", got, test.wantQuery)
			}
		})
	}
}

// The erratum canonicalizes any GET carrying a non-empty cursor to that cursor
// alone, which is broader than the single V2ListLedgers defect that motivated
// it. That breadth is only safe while no bound operation legitimately sends a
// continuation cursor beside another query parameter, and nothing checked it.
//
// This pins the generated client's own behaviour, ahead of the erratum. Two of
// the five paginated operations inject generated defaults beside the cursor —
// not one, as the erratum's original note said. Every injected value is a
// client-side default the adapter never asked for; the three remaining
// operations send the cursor alone. So the canonicalization currently drops
// only defaults, never a caller value.
//
// If a regenerated client starts combining a cursor with a parameter that
// carries caller intent, this test fails and the erratum must be narrowed
// before that parameter is silently dropped.
func TestGeneratedClientSendsOnlyTheCursorOnEveryPaginatedContinuation(t *testing.T) {
	t.Parallel()

	cursor := "opaque-2"
	tests := []struct {
		name      string
		call      func(context.Context, *ledgerclient.V2) error
		wantQuery string
	}{
		{name: "v2ListLedgers", wantQuery: "cursor=opaque-2&includeDeleted=false", call: func(ctx context.Context, v2 *ledgerclient.V2) error {
			_, err := v2.ListLedgers(ctx, operations.V2ListLedgersRequest{Cursor: &cursor})
			return err
		}},
		{name: "v2ListAccounts", wantQuery: "cursor=opaque-2", call: func(ctx context.Context, v2 *ledgerclient.V2) error {
			_, err := v2.ListAccounts(ctx, operations.V2ListAccountsRequest{Ledger: "primary", Cursor: &cursor})
			return err
		}},
		{name: "v2ListTransactions", wantQuery: "cursor=opaque-2", call: func(ctx context.Context, v2 *ledgerclient.V2) error {
			_, err := v2.ListTransactions(ctx, operations.V2ListTransactionsRequest{Ledger: "primary", Cursor: &cursor})
			return err
		}},
		{name: "v2GetVolumesWithBalances", wantQuery: "cursor=opaque-2", call: func(ctx context.Context, v2 *ledgerclient.V2) error {
			_, err := v2.GetVolumesWithBalances(ctx, operations.V2GetVolumesWithBalancesRequest{Ledger: "primary", Cursor: &cursor})
			return err
		}},
		{name: "v2ListSchemas", wantQuery: "cursor=opaque-2&order=desc&pageSize=15&sort=created_at", call: func(ctx context.Context, v2 *ledgerclient.V2) error {
			_, err := v2.ListSchemas(ctx, operations.V2ListSchemasRequest{Ledger: "primary", Cursor: &cursor})
			return err
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			capture := &captureHTTPClient{}
			client := ledgerclient.New(
				ledgerclient.WithClient(capture),
				ledgerclient.WithServerURL("https://product.invalid"),
				ledgerclient.WithSecuritySource(func(context.Context) (components.Security, error) {
					return components.Security{}, nil
				}),
			)
			// The capture answers 204, which is not a declared success for
			// any of these operations, so the call errors after the request
			// is built. The request is the subject; the response is not.
			_ = test.call(context.Background(), client.Ledger.V2)
			if capture.request == nil {
				t.Fatal("generated client issued no request")
			}
			if got := capture.request.URL.RawQuery; got != test.wantQuery {
				t.Fatalf("continuation query = %q, want %q", got, test.wantQuery)
			}
		})
	}
}
