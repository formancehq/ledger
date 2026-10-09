//go:build host

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// TestFctlLifecycle exercises the public CLI boundary without importing fctl.
// Supply a trusted, already-built fctl v4 executable through FCTL_BINARY.
func TestFctlLifecycle(t *testing.T) {
	t.Parallel()
	host, err := filepath.Abs(os.Getenv("FCTL_BINARY"))
	if err != nil || os.Getenv("FCTL_BINARY") == "" {
		t.Fatal("FCTL_BINARY must name the fctl v4 executable")
	}
	binary := filepath.Join(t.TempDir(), "fctl-plugin-ledger")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "-ldflags", "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=1", ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %s %v", output, err)
	}
	var version atomic.Value
	version.Store("3.0.0-beta.10")
	var mutations, requests atomic.Int32
	const amount = "90071992547409930001"
	const payload = `{"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":` + amount + `}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		var err error
		switch r.URL.Path {
		case "/gateway/ledger/_info":
			err = json.NewEncoder(w).Encode(map[string]any{"version": version.Load()})
		case "/gateway/ledger/v3/":
			_, err = io.WriteString(w, `{"data":[{"name":"books"}]}`)
		case "/gateway/ledger/v3/books/transactions":
			mutations.Add(1)
			body, readErr := io.ReadAll(r.Body)
			if readErr != nil || string(body) != payload || r.Method != http.MethodPost {
				t.Errorf("changed mutation: %s %s %v", r.Method, body, readErr)
			}
			w.WriteHeader(http.StatusCreated)
			_, err = io.WriteString(w, `{"data":{"id":`+amount+`}}`)
		case "/gateway/ledger/v3/books/bulk":
			mutations.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, err = io.WriteString(w, `{"data":[{"responseType":"CREATE_TRANSACTION","data":{"id":`+amount+`}},{"responseType":"ERROR","errorCode":"INSUFFICIENT_FUNDS"}],"errorCode":"BULK_FAILED","errorMessage":"partial failure"}`)
		default:
			t.Errorf("unexpected route: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		if err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	args := []string{"--config-dir", t.TempDir(), "--auth-mode", "none", "--ledger-url", server.URL + "/gateway/ledger", "--no-input", "-o", "json"}
	catalogue := filepath.Join(t.TempDir(), "catalogue.json")
	if err := os.WriteFile(catalogue, []byte(`{"schemaVersion":1,"releases":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	runHost := func(extra ...string) (string, error) {
		t.Helper()
		command := exec.CommandContext(t.Context(), host, append(append([]string{}, args...), extra...)...)
		command.Env = append(os.Environ(), "FCTL_PLUGIN_CATALOGUE="+catalogue)
		output, err := command.CombinedOutput()

		return string(output), err
	}
	out, err := runHost("plugins", "install", "--service", "ledger", "--binary", binary)
	if err != nil || !strings.Contains(out, `"source": "local"`) {
		t.Fatalf("install: %s %v", out, err)
	}
	out, err = runHost("plugins", "show", "--service", "ledger")
	if err != nil || !strings.Contains(out, "3.0.0-beta.10") {
		t.Fatalf("show: %s %v", out, err)
	}
	out, err = runHost("ledger", "list")
	if err != nil || !strings.Contains(out, "books") {
		t.Fatalf("list: %s %v", out, err)
	}
	out, err = runHost("ledger", "transactions", "create", "--ledger", "books", "--data", payload)
	if err != nil || !strings.Contains(out, amount) || mutations.Load() != 1 {
		t.Fatalf("create: %s %v mutations=%d", out, err, mutations.Load())
	}
	out, err = runHost("ledger", "bulk", "--ledger", "books", "--data", "[]")
	if err == nil || !strings.Contains(out, amount) || !strings.Contains(out, "BULK_FAILED") || mutations.Load() != 2 {
		t.Fatalf("partial result: %s %v mutations=%d", out, err, mutations.Load())
	}
	version.Store("3.0.0-beta.11")
	out, err = runHost("ledger", "transactions", "create", "--ledger", "books", "--data", payload)
	if err == nil || !strings.Contains(out, "exact service version") || mutations.Load() != 2 {
		t.Fatalf("version drift reached mutation: %s %v mutations=%d", out, err, mutations.Load())
	}
	server.Close()
	before := requests.Load()
	for _, extra := range [][]string{{"ledger", "--help"}, {"ledger", "transactions", "create", "--help"}, {"__complete", "ledger", "transactions", ""}} {
		out, err = runHost(extra...)
		if err != nil || out == "" || requests.Load() != before {
			t.Fatalf("offline metadata %v: %s %v", extra, out, err)
		}
		if extra[0] == "__complete" && !strings.Contains(out, "create") {
			t.Fatalf("missing completion: %s", out)
		}
	}
}
