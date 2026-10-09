package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	"github.com/formancehq/fctl/pkg/pluginsdk/httpclient"
	"github.com/formancehq/fctl/pkg/pluginsdk/transport"
)

type authenticatedTransport struct{ base http.RoundTripper }

func (t authenticatedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	request := req.Clone(req.Context())
	request.Header.Set("Authorization", "Bearer synthetic-host-token")

	return t.base.RoundTrip(request)
}

func TestStandaloneExecutableUsesHostHTTPBroker(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "fctl-plugin-ledger")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary,
		"-ldflags", "-X main.serviceVersion=3.0.0-beta.5 -X main.revision=2", ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone executable: %s, %v", output, err)
	}
	const payload = `{ "postings": [{"source":"world","destination":"users:42","asset":"USD/2","amount":123456789012345678901234567890}] }`
	const result = `{ "data": { "id": 123456789012345678901234567890 } }`
	const partial = `{ "data": [{"responseType":"ERROR","errorCode":"REJECTED"}], "errorCode":"BULK_FAILED", "errorMessage":"partial failure" }`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer synthetic-host-token" {
			t.Error("host authentication was not applied")
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/v3/books/transactions":
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != payload || req.Method != http.MethodPost {
				t.Errorf("request changed: method=%s, body=%s, err=%v", req.Method, body, err)
			}
			w.WriteHeader(http.StatusCreated)
			if _, err := io.WriteString(w, result); err != nil {
				t.Error(err)
			}
		case "/v3/books/bulk":
			w.WriteHeader(http.StatusBadRequest)
			if _, err := io.WriteString(w, partial); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected route: %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := &http.Client{Transport: authenticatedTransport{base: http.DefaultTransport}}
	plugin, err := transport.Open(t.Context(), binary, client, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := plugin.Close(); err != nil {
			t.Error(err)
		}
	}()
	manifest, err := plugin.GetManifest(t.Context())
	if err != nil || manifest.Version != "3.0.0-beta.5" {
		t.Fatalf("standalone manifest=%#v, err=%v", manifest, err)
	}
	if _, err := pluginsdk.FindCommand(manifest, []string{"ledger", "transactions", "create"}); err != nil {
		t.Fatal(err)
	}
	response, err := plugin.Execute(t.Context(), pluginsdk.ExecuteRequest{
		CommandPath: []string{"ledger", "transactions", "create"},
		Flags:       map[string]string{"ledger": "books"},
		Body:        json.RawMessage(payload), Endpoint: server.URL,
	})
	if err != nil || string(response.Data) != result {
		t.Fatalf("standalone result=%s, err=%v", response.Data, err)
	}
	response, err = plugin.Execute(t.Context(), pluginsdk.ExecuteRequest{
		CommandPath: []string{"ledger", "bulk"}, Flags: map[string]string{"ledger": "books"},
		Body: json.RawMessage("[]"), Endpoint: server.URL,
	})
	failure, ok := errors.AsType[*httpclient.Error](err)
	if !ok || failure.StatusCode != http.StatusBadRequest || !strings.Contains(failure.Code, "BULK_FAILED") || string(response.Data) != partial {
		t.Fatalf("partial standalone result=%s, err=%v", response.Data, err)
	}
}
