package ledger

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestManifestContract(t *testing.T) {
	t.Parallel()
	p := New(nil)
	manifest, err := p.GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != "ledger" || manifest.Service != "ledger" || manifest.Version == "" || manifest.ProtocolVersion != pluginsdk.ProtocolVersion {
		t.Fatalf("manifest identity: %#v", manifest)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pluginsdk.Manifest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifest, decoded) {
		t.Fatal("manifest does not survive protocol serialization")
	}
	create, err := pluginsdk.FindCommand(manifest, []string{"ledger", "transactions", "create"})
	if err != nil {
		t.Fatal(err)
	}
	checkBodyFlag(t, create)
	deleteCommand, err := pluginsdk.FindCommand(manifest, []string{"ledger", "accounts", "metadata", "delete"})
	if err != nil || !deleteCommand.Confirm || deleteCommand.Args.Min != 2 || deleteCommand.Args.Max != 2 {
		t.Fatalf("delete contract: %#v, %v", deleteCommand, err)
	}
	manifest.Root.Flags[0].Name = "corrupted"
	fresh, err := p.GetManifest(t.Context())
	if err != nil || fresh.Root.Flags[0].Name != "ledger" {
		t.Fatal("caller mutated subsequent manifests")
	}
}

func checkBodyFlag(t *testing.T, spec pluginsdk.CommandSpec) {
	t.Helper()
	for _, flag := range spec.Flags {
		if flag.Name == "data" {
			if !flag.Required || !flag.Body || flag.Type != "string" {
				t.Fatalf("data flag: %#v", flag)
			}

			return
		}
	}
	t.Fatal("transaction creation missing data flag")
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestChangedFlagsAndRequestIsolation(t *testing.T) {
	t.Parallel()
	p := New(&http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		query := req.URL.Query()
		if query.Has("reverse") || query.Get("pageSize") != "100" {
			t.Errorf("unchanged flag or default query: %v", query)
		}

		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(envelope))}, nil
	})})
	req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "transactions", "list"}, Flags: map[string]string{"ledger": "books", "reverse": "true"}, Endpoint: "https://ledger.example/prefix"}
	result, err := p.Execute(t.Context(), req)
	if err != nil || string(result.Data) != envelope {
		t.Fatalf("result=%s err=%v", result.Data, err)
	}
	if _, exists := req.Flags["page-size"]; exists {
		t.Fatal("Execute mutated caller flags")
	}
}

func TestDirectSDKRejectsMissingBody(t *testing.T) {
	t.Parallel()
	p := New(nil)
	req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "transactions", "create"}, Flags: map[string]string{"ledger": "books", "data": "@unread.json"}, Endpoint: "https://ledger.example"}
	if _, err := p.Execute(t.Context(), req); err == nil {
		t.Fatal("plugin attempted to execute unread body")
	}
}

func TestDirectSDKAcceptsBodyWithoutDataFlag(t *testing.T) {
	t.Parallel()
	body := `{"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":123456789012345678901234567890}]}`
	p := New(&http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		data, err := io.ReadAll(req.Body)
		if err != nil || string(data) != body {
			t.Errorf("request body=%s err=%v", data, err)
		}

		return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(envelope))}, nil
	})})
	req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "transactions", "create"}, Flags: map[string]string{"ledger": "books"}, Body: json.RawMessage(body), Endpoint: "https://ledger.example"}
	response, err := p.Execute(t.Context(), req)
	if err != nil || string(response.Data) != envelope {
		t.Fatalf("direct SDK body: response=%s err=%v", response.Data, err)
	}
}

func TestBulkPartialHTTPResponsePreserved(t *testing.T) {
	t.Parallel()
	body := `{"data":[{"responseType":"CREATE_TRANSACTION","data":{"amount":123456789012345678901234567890}},{"responseType":"ERROR","errorCode":"INSUFFICIENT_FUNDS"}],"errorCode":"BULK_FAILED","errorMessage":"partial failure"}`
	out, reqs, err := execute(t, []string{"--ledger", "books", "bulk", "--data", "[]", "--idempotency-key", "batch-42"}, "", body, 400)
	if err == nil || len(reqs) != 1 || out != body {
		t.Fatalf("partial results lost: out=%s calls=%d err=%v", out, len(reqs), err)
	}
}

func TestHTTPFailureResponseParity(t *testing.T) {
	t.Parallel()
	body := `{"data":[{"responseType":"ERROR","errorCode":"REJECTED"}],"errorCode":"REJECTED","errorMessage":"request refused"}`
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"nonbulk response empty", []string{"--ledger", "books", "transactions", "create", "--data", "{}"}, ""},
		{"bulk partial response retained", []string{"--ledger", "books", "bulk", "--data", "[]"}, body},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, reqs, err := execute(t, tc.args, "", body, 400)
			if err == nil || out != tc.want || len(reqs) != 1 {
				t.Fatalf("error response parity: Data=%q want=%q calls=%d err=%v", out, tc.want, len(reqs), err)
			}
		})
	}
}

type closeErrorReader struct {
	io.Reader

	err error
}

func (r closeErrorReader) Close() error { return r.err }

func TestSuccessfulJSONWithCloseError(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("response close failed")
	body := `{"data":[{"responseType":"CREATE_TRANSACTION","data":{"amount":123456789012345678901234567890}}]}`
	cases := []struct {
		name string
		path []string
		want string
	}{
		{"nonbulk clears data", []string{"ledger", "transactions", "create"}, ""},
		{"bulk retains data", []string{"ledger", "bulk"}, body},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			p := New(&http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
				calls++

				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: closeErrorReader{Reader: strings.NewReader(body), err: closeErr}}, nil
			})})
			req := pluginsdk.ExecuteRequest{CommandPath: tc.path, Flags: map[string]string{"ledger": "books"}, Body: json.RawMessage(`[]`), Endpoint: "https://ledger.example"}
			response, err := p.Execute(t.Context(), req)
			if !errors.Is(err, closeErr) || string(response.Data) != tc.want || calls != 1 {
				t.Fatalf("close error: Data=%q want=%q calls=%d err=%v", response.Data, tc.want, calls, err)
			}
		})
	}
}
