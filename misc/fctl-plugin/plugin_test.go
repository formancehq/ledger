package ledger

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

type request struct {
	method, path, body string
	query              url.Values
	header             http.Header
}

const envelope = `{"data":{"amount":1234567890123456789012345678901234567890,"balance":"1234567890123456789012345678901234567890"},"next":"next/+=","previous":"previous","hasMore":true}`

func execute(t *testing.T, args []string, input, response string, status int) (string, []request, error) {
	t.Helper()
	requests := make(chan request, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		requests <- request{r.Method, r.URL.EscapedPath(), string(body), r.URL.Query(), r.Header.Clone()}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if _, err := io.WriteString(w, response); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	p := New(server.Client())
	manifest, err := p.GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest(manifest, args, input)
	req.Endpoint = server.URL + "/gateway/ledger"
	result, err := p.Execute(t.Context(), req)
	var got []request
	for len(requests) > 0 {
		got = append(got, <-requests)
	}

	return string(result.Data), got, err
}

type routeCase struct {
	name, method, path string
	args               []string
	body               string
}

func TestV3Routes(t *testing.T) {
	t.Parallel()
	cases := []routeCase{
		{"ledgers", "GET", "/v3/", []string{"list"}, ""},
		{"create", "POST", "/v3/books", []string{"create", "books"}, "{}"},
		{"create selected", "POST", "/v3/books", []string{"--ledger", "books", "create", "--data", `{"metadata":{"owner":"test"}}`}, `{"metadata":{"owner":"test"}}`},
		{"show", "GET", "/v3/books", []string{"show", "books"}, ""},
		{"show selected", "GET", "/v3/books", []string{"--ledger", "books", "show"}, ""},
		{"delete", "DELETE", "/v3/books", []string{"delete", "books", "--confirm"}, ""},
		{"stats", "GET", "/v3/books/stats", []string{"stats", "--ledger", "books"}, ""},
		{"info", "GET", "/_info", []string{"info"}, ""},
		{"accounts", "GET", "/v3/books/accounts", []string{"accounts", "list"}, ""},
		{"account", "GET", "/v3/books/accounts/users:42", []string{"accounts", "show", "users:42"}, ""},
		{"account balances", "GET", "/v3/books/accounts/users:42", []string{"accounts", "balances", "users:42"}, ""},
		{"balances", "GET", "/v3/books/volumes", []string{"balances"}, ""},
		{"transactions", "GET", "/v3/books/transactions", []string{"transactions", "list"}, ""},
		{"transaction", "GET", "/v3/books/transactions/18446744073709551615", []string{"transactions", "show", "18446744073709551615"}, ""},
		{"transaction create", "POST", "/v3/books/transactions", []string{"transactions", "create", "--data", `{"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":123456789012345678901234567890}]}`}, `{"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":123456789012345678901234567890}]}`},
		{"revert", "POST", "/v3/books/transactions/42/revert", []string{"transactions", "revert", "42"}, ""},
		{"revert body", "POST", "/v3/books/transactions/42/revert", []string{"transactions", "revert", "42", "--data", `{"metadata":{"reason":"test"}}`}, `{"metadata":{"reason":"test"}}`},
		{"ledger metadata read", "GET", "/v3/books", []string{"metadata", "show"}, ""},
		{"ledger metadata set", "POST", "/v3/books/metadata", []string{"metadata", "set", "--data", `{"score":9007199254740993}`}, `{"score":9007199254740993}`},
		{"ledger metadata delete", "DELETE", "/v3/books/metadata/formance.com%2Freviewed", []string{"metadata", "delete", "formance.com/reviewed", "--confirm"}, ""},
		{"account metadata read", "GET", "/v3/books/accounts/users:42", []string{"accounts", "metadata", "show", "users:42"}, ""},
		{"account metadata set", "POST", "/v3/books/accounts/users:42/metadata", []string{"accounts", "metadata", "set", "users:42", "--data", `{"score":9007199254740993}`}, `{"score":9007199254740993}`},
		{"account metadata delete", "DELETE", "/v3/books/accounts/users:42/metadata/formance.com%2Freviewed", []string{"accounts", "metadata", "delete", "users:42", "formance.com/reviewed", "--confirm"}, ""},
		{"transaction metadata read", "GET", "/v3/books/transactions/42", []string{"transactions", "metadata", "show", "42"}, ""},
		{"transaction metadata set", "POST", "/v3/books/transactions/42/metadata", []string{"transactions", "metadata", "set", "42", "--data", `{"score":9007199254740993}`}, `{"score":9007199254740993}`},
		{"transaction metadata delete", "DELETE", "/v3/books/transactions/42/metadata/a%2Fb", []string{"transactions", "metadata", "delete", "42", "a/b", "--confirm"}, ""},
		{"logs", "GET", "/v3/books/logs", []string{"logs", "list"}, ""},
		{"bulk", "POST", "/v3/books/bulk", []string{"bulk", "--data", `[{"action":"CREATE_TRANSACTION","ik":"payment-42","data":{"postings":[]}}]`}, `[{"action":"CREATE_TRANSACTION","ik":"payment-42","data":{"postings":[]}}]`},
		{"indexes", "GET", "/v3/books/indexes", []string{"indexes", "list"}, ""},
		{"index", "GET", "/v3/books/indexes/metadata:TARGET_TYPE_ACCOUNT:a%2Fb", []string{"indexes", "show", "metadata:TARGET_TYPE_ACCOUNT:a/b"}, ""},
		{"index status", "GET", "/v3/books/indexes/metadata:TARGET_TYPE_ACCOUNT:a%2Fb/status", []string{"indexes", "status", "metadata:TARGET_TYPE_ACCOUNT:a/b"}, ""},
		{"index create", "POST", "/v3/books/indexes", []string{"indexes", "create", "--data", `{"id":"metadata:TARGET_TYPE_ACCOUNT:a/b"}`}, `{"id":"metadata:TARGET_TYPE_ACCOUNT:a/b"}`},
		{"index delete", "DELETE", "/v3/books/indexes/metadata:TARGET_TYPE_ACCOUNT:a%2Fb", []string{"indexes", "delete", "metadata:TARGET_TYPE_ACCOUNT:a/b", "--confirm"}, ""},
		{"index inspect", "GET", "/v3/books/indexes/metadata:TARGET_TYPE_ACCOUNT:a%2Fb/inspect", []string{"indexes", "inspect", "metadata:TARGET_TYPE_ACCOUNT:a/b"}, ""},
		{"raw ledger", "GET", "/v3/a%2Fb%25%20c", []string{"show", "a/b% c"}, ""},
		{"raw address", "GET", "/v3/books/accounts/users:a%2Fb%25%20c", []string{"accounts", "show", "users:a/b% c"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			checkRoute(t, tc)
		})
	}
}

func routeArguments(tc routeCase) []string {
	args := slices.Clone(tc.args)
	unscoped := []string{"ledgers", "info", "raw ledger", "create selected", "show selected", "create", "show", "delete"}
	if !slices.Contains(unscoped, tc.name) {
		args = append([]string{"--ledger", "books"}, args...)
	}
	if tc.method != http.MethodGet {
		args = append(args, "--idempotency-key", "operation-42")
	}

	return args
}

func checkRoute(t *testing.T, tc routeCase) {
	t.Helper()
	response := envelope
	if tc.name == "bulk" {
		response = strings.Replace(envelope, `"data":{`, `"data":[{`, 1)
		response = strings.Replace(response, `},"next"`, `}],"next"`, 1)
	}
	out, requests, err := execute(t, routeArguments(tc), "", response, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want exactly one", len(requests))
	}
	checkRequest(t, requests[0], tc)
	for _, value := range []string{"1234567890123456789012345678901234567890", `"next"`, `"previous"`, `"hasMore"`} {
		if !strings.Contains(out, value) {
			t.Errorf("output lost %s: %s", value, out)
		}
	}
}

func checkRequest(t *testing.T, got request, tc routeCase) {
	t.Helper()
	if got.method != tc.method || got.path != "/gateway/ledger"+tc.path || got.body != tc.body {
		t.Fatalf("request = %#v; want %s %s body %s", got, tc.method, tc.path, tc.body)
	}
	if tc.body != "" && got.header.Get("Content-Type") != "application/json" {
		t.Error("missing JSON content type")
	}
	if tc.method != http.MethodGet && got.header.Get("Idempotency-Key") != "operation-42" {
		t.Error("write lost its idempotency identity")
	}
}

func TestQueryAndHeaders(t *testing.T) {
	t.Parallel()
	for _, collection := range []string{"transactions", "logs", "accounts"} {
		t.Run(collection, func(t *testing.T) { checkListQuery(t, collection) })
	}
}

func checkListQuery(t *testing.T, collection string) {
	t.Helper()
	after := "18446744073709551615"
	if collection == "accounts" {
		after = "users:next/+="
	}
	args := []string{"--ledger", "books", "--consistency", " STALE ", collection, "list", "--page-size", "0", "--after", after, "--filter", `metadata[score] >= 9007199254740993`}
	cursor := base64.RawURLEncoding.EncodeToString([]byte(`{"key":"` + after + `"}`))
	want := map[string]string{"pageSize": "0", "cursor": cursor, "filter": `metadata[score] >= 9007199254740993`}
	if collection != "logs" {
		args = append(args, "--reverse")
		want["reverse"] = "true"
	}
	if collection != "accounts" {
		args = append(args, "--start-date", "2026-10-01T00:00:00Z", "--end-date", "2026-10-08T12:00:00+02:00")
		want["startDate"] = "2026-10-01T00:00:00Z"
		want["endDate"] = "2026-10-08T12:00:00+02:00"
	}
	_, reqs, err := execute(t, args, "", envelope, 200)
	if err != nil {
		t.Fatal(err)
	}
	checkQuery(t, reqs[0].query, want)
	if len(reqs[0].query) != len(want) || reqs[0].query.Has("after") || (collection == "logs" && reqs[0].query.Has("reverse")) {
		t.Fatalf("unsupported query keys: %v", reqs[0].query)
	}
	if reqs[0].header.Get("X-Consistency") != "stale" {
		t.Error("missing read consistency")
	}
}

func checkQuery(t *testing.T, got url.Values, want map[string]string) {
	t.Helper()
	for key, value := range want {
		if got.Get(key) != value {
			t.Errorf("query %s = %q, want %q", key, got.Get(key), value)
		}
	}
}

func TestAggregateQuery(t *testing.T) {
	t.Parallel()
	_, reqs, err := execute(t, []string{"--ledger", "books", "balances", "--collapse-colors", "--use-max-precision", "--group-by-prefixes", "users:,merchants:", "--filter", `{"$match":{"address":"users:42"}}`}, "", envelope, 200)
	if err != nil {
		t.Fatal(err)
	}
	checkQuery(t, reqs[0].query, map[string]string{"collapseColors": "true", "useMaxPrecision": "true", "groupByPrefixes": "users:,merchants:", "filter": `{"$match":{"address":"users:42"}}`})
}

func TestIndexInspectionQuery(t *testing.T) {
	t.Parallel()
	_, reqs, err := execute(t, []string{"--ledger", "books", "indexes", "inspect", "metadata:TARGET_TYPE_ACCOUNT:key", "--mode", "facets", "--page-size", "10000", "--cursor", "previous/+="}, "", envelope, 200)
	if err != nil {
		t.Fatal(err)
	}
	checkQuery(t, reqs[0].query, map[string]string{"mode": "facets", "pageSize": "10000", "cursor": "previous/+="})
	if reqs[0].query.Has("after") {
		t.Fatalf("index inspection sent entity pagination: %v", reqs[0].query)
	}
}

func TestBodiesAndIdempotency(t *testing.T) {
	t.Parallel()
	body := `{"postings":[],"metadata":{"number":9007199254740993}}`
	// The host already read all three inputs. The plugin only receives JSON.
	for _, data := range []string{body, "@already-read.json", "-"} {
		_, reqs, err := execute(t, []string{"--ledger", "books", "transactions", "create", "--data", data, "--idempotency-key", "payment-42"}, body, envelope, 201)
		if err != nil {
			t.Fatal(err)
		}
		if len(reqs) != 1 || reqs[0].body != body || reqs[0].header.Get("Idempotency-Key") != "payment-42" {
			t.Fatalf("body or identity changed: %#v", reqs)
		}
	}
	_, reqs, err := execute(t, []string{"--ledger", "books", "bulk", "--data", "[]", "--atomic", "--continue-on-failure=false", "--idempotency-key", "batch-42"}, "", `{"data":[]}`, 200)
	if err != nil || reqs[0].query.Get("atomic") != "true" || reqs[0].query.Get("continueOnFailure") != "false" || reqs[0].header.Get("Idempotency-Key") != "batch-42" {
		t.Fatalf("bulk: %v, %v", reqs, err)
	}
}

func TestValidationBeforeHTTP(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"accounts", "list"}, {"create"}, {"--ledger", "books", "show", "other"},
		{"--ledger", "books", "accounts", "show"}, {"--ledger", "books", "accounts", "show", ""},
		{"--ledger", "books", "transactions", "show", "-1"}, {"--ledger", "books", "transactions", "show", "18446744073709551616"},
		{"--ledger", "books", "transactions", "metadata", "set", "abc", "--data", "{}"},
		{"--ledger", "books", "transactions", "create"}, {"--ledger", "books", "transactions", "create", "--data", "{} {}"},
		{"--ledger", "books", "transactions", "revert", "42", "--data", ""},
		{"--ledger", "books", "transactions", "create", "--data", "@/nonexistent/fctl/request.json"},
		{"delete", "books"}, {"delete", "books", "--confirm=false"},
		{"--ledger", "books", "metadata", "delete", "key"}, {"--ledger", "books", "indexes", "delete", "index"},
		{"--ledger", "books", "accounts", "list", "--page-size", "-1"},
		{"--ledger", "books", "transactions", "list", "--start-date", "yesterday"},
		{"--ledger", "books", "logs", "list", "--end-date", "1969-12-31T00:00:00Z"},
		{"--ledger", "books", "indexes", "inspect", "index", "--mode", "bad"},
		{"--ledger", "books", "indexes", "inspect", "index", "--page-size", "0"},
		{"--ledger", "books", "indexes", "inspect", "index", "--page-size", "10001"},
		{"--ledger", "books", "--consistency", "eventual", "stats"},
		{"create", "books", "--idempotency-key", strings.Repeat("a", 257)},
		{"create", "books", "--idempotency-key", "a\nb"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_, reqs, err := execute(t, args, "", envelope, 200)
			if err == nil || len(reqs) != 0 {
				t.Fatalf("want validation error before HTTP, got requests=%v err=%v", reqs, err)
			}
		})
	}
}

func TestHTTPFailuresNeverRetryMutations(t *testing.T) {
	t.Parallel()
	for _, status := range []int{400, 401, 403, 404, 409, 429, 500, 503} {
		out, reqs, err := execute(t, []string{"--ledger", "books", "transactions", "create", "--data", "{}", "--idempotency-key", "payment-42"}, "", `{"errorCode":"TEST_ERROR","errorMessage":"write refused"}`, status)
		if err == nil || !strings.Contains(err.Error(), "TEST_ERROR") || !strings.Contains(err.Error(), "write refused") || len(reqs) != 1 || out != "" {
			t.Fatalf("status %d: output=%s requests=%d err=%v", status, out, len(reqs), err)
		}
	}
	_, reqs, err := execute(t, []string{"show", "books"}, "", "not json", 200)
	if err == nil || len(reqs) != 1 {
		t.Fatalf("invalid response: requests=%d err=%v", len(reqs), err)
	}
	out, reqs, err := execute(t, []string{"delete", "books", "--confirm", "--idempotency-key", "delete-books"}, "", "", 204)
	if err != nil || out != "null" || len(reqs) != 1 || reqs[0].header.Get("Idempotency-Key") != "delete-books" {
		t.Fatalf("204 response: output=%s requests=%v err=%v", out, reqs, err)
	}
}

func TestEndpointAndContextErrors(t *testing.T) {
	t.Parallel()
	p := New(nil)
	_, err := p.Execute(t.Context(), pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "list"}, Endpoint: "invalid"})
	if err == nil {
		t.Fatal("invalid endpoint accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.GetManifest(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("manifest context error = %v", err)
	}
}

func TestBulkBusinessFailuresAreNonzero(t *testing.T) {
	t.Parallel()
	for _, response := range []string{
		`{"data":[{"responseType":"CREATE_TRANSACTION","data":{"id":42}},{"responseType":"ERROR","errorCode":"INSUFFICIENT_FUNDS","errorDescription":"not enough funds"}]}`,
		`{"data":[],"errorCode":"BATCH_FAILED","errorMessage":"batch refused"}`,
		`{"data":{}}`,
	} {
		out, reqs, err := execute(t, []string{"--ledger", "books", "bulk", "--data", "[]", "--continue-on-failure"}, "", response, 200)
		if err == nil || len(reqs) != 1 || out == "" {
			t.Fatalf("bulk failures must keep JSON and fail once: out=%s requests=%d err=%v", out, len(reqs), err)
		}
	}
	_, reqs, err := execute(t, []string{"--ledger", "books", "bulk", "--data", "[]"}, "", `{"errorCode":"UNAVAILABLE","errorMessage":"leader lost"}`, 503)
	if err == nil || len(reqs) != 1 {
		t.Fatalf("bulk HTTP failure retried or ignored: requests=%d err=%v", len(reqs), err)
	}
}

func TestPageSizeUsesV3ServerSemantics(t *testing.T) {
	t.Parallel()
	_, reqs, err := execute(t, []string{"--ledger", "books", "accounts", "list", "--page-size", "1001", "--reverse=false", "--consistency", "linearizable"}, "", `{"data":[],"hasMore":false}`, 200)
	if err != nil || reqs[0].query.Get("pageSize") != "1001" || reqs[0].query.Get("reverse") != "false" || reqs[0].header.Get("X-Consistency") != "linearizable" {
		t.Fatalf("page options = %v, err=%v", reqs, err)
	}
	_, reqs, err = execute(t, []string{"accounts", "balances", "users:42", "--ledger", "books", "--collapse-colors=false"}, "", envelope, 200)
	if err != nil || reqs[0].query.Get("collapseColors") != "false" {
		t.Fatalf("account options = %v, err=%v", reqs, err)
	}
}
