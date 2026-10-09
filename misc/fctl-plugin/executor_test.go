package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestExecutorPreservesPreparedJSONAndPartialResults(t *testing.T) {
	t.Parallel()
	const body = `{"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":9007199254740993}]}`
	const partial = `{"data":{"id":9007199254740993},"errorCode":"PARTIAL_FAILURE"}`
	failure := errors.New("accepted write, response delivery failed")
	calls := 0
	p := NewWithExecutor("3.0.0-beta.10", func(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
		calls++
		if ctx != t.Context() || req.Endpoint != "grpc://ledger.example:8080" || string(req.Body) != body {
			t.Fatalf("executor changed context, endpoint or exact JSON: %#v", req)
		}
		if req.Flags["data"] != "@already-read.json" || !req.ChangedFlags["data"] || req.Flags["idempotency-key"] != "payment-42" || req.Flags["consistency"] != "" {
			t.Fatalf("executor request was not normalized or lost flags: %#v", req)
		}
		if req.Context["profile"] != "local" {
			t.Fatalf("host context lost: %#v", req.Context)
		}

		return pluginsdk.ExecuteResponse{Data: json.RawMessage(partial)}, failure
	})
	req := pluginsdk.ExecuteRequest{
		CommandPath:  []string{"ledger", "transactions", "create"},
		Flags:        map[string]string{"ledger": "books", "data": "@already-read.json", "idempotency-key": "payment-42"},
		ChangedFlags: map[string]bool{"data": true},
		Body:         json.RawMessage(body), Endpoint: "grpc://ledger.example:8080", Context: map[string]string{"profile": "local"},
	}
	response, err := p.Execute(t.Context(), req)
	if !errors.Is(err, failure) || string(response.Data) != partial || calls != 1 {
		t.Fatalf("partial result, exact number or single attempt lost: response=%s calls=%d err=%v", response.Data, calls, err)
	}
	if _, changed := req.Flags["consistency"]; changed {
		t.Fatal("normalization changed the caller's flags")
	}
}

func TestExecutorReceivesDefaultsAndExplicitFalse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		req  pluginsdk.ExecuteRequest
	}{
		{"read defaults", pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "transactions", "list"}, Flags: map[string]string{"ledger": "books", "reverse": "false"}, ChangedFlags: map[string]bool{"reverse": true}}},
		{"create default body", pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "create"}, Args: []string{"books"}}},
		{"read ignores unused body", pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "list"}, Body: json.RawMessage(`{"unused":true}`)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			p := NewWithExecutor("", func(_ context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
				calls++
				switch tc.name {
				case "read defaults":
					if req.Flags["page-size"] != "100" || req.Flags["reverse"] != "false" || !req.ChangedFlags["reverse"] || req.Body != nil {
						t.Fatalf("default or explicit false lost: %#v", req)
					}
				case "create default body":
					if string(req.Body) != "{}" || req.Flags["data"] != "{}" || req.ChangedFlags["data"] {
						t.Fatalf("default body not prepared: %#v", req)
					}
				case "read ignores unused body":
					if req.Body != nil {
						t.Fatalf("read received a body: %s", req.Body)
					}
				}

				return pluginsdk.ExecuteResponse{Data: json.RawMessage(`{"data":[]}`)}, nil
			})
			if _, err := p.Execute(t.Context(), tc.req); err != nil || calls != 1 {
				t.Fatalf("execute: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestExecutorRejectsInvalidCommandsBeforeDispatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		tokens []string
		body   string
		want   string
	}{
		{"unknown command", []string{"unknown"}, "", "not executable"},
		{"unknown flag", []string{"list", "--unknown", "value"}, "", "unknown plugin flag"},
		{"missing ledger", []string{"accounts", "list"}, "", "select a ledger"},
		{"conflicting ledger", []string{"--ledger", "books", "show", "other"}, "", "conflicts with --ledger"},
		{"missing argument", []string{"--ledger", "books", "accounts", "show"}, "", "command expects"},
		{"empty identifier", []string{"--ledger", "books", "accounts", "show", ""}, "", "identifiers must not be empty"},
		{"invalid transaction ID", []string{"--ledger", "books", "transactions", "show", "-1"}, "", "unsigned 64-bit integer"},
		{"overflow transaction ID", []string{"--ledger", "books", "transactions", "show", "18446744073709551616"}, "", "unsigned 64-bit integer"},
		{"malformed JSON", []string{"--ledger", "books", "transactions", "create"}, "{", "valid JSON"},
		{"multiple JSON values", []string{"--ledger", "books", "transactions", "create"}, "{} {}", "valid JSON"},
		{"oversized body", []string{"--ledger", "books", "transactions", "create"}, `"` + strings.Repeat("x", 4<<20) + `"`, "within 4 MiB"},
		{"unread file", []string{"--ledger", "books", "transactions", "create", "--data", "@/nonexistent/request.json"}, "", "already-read JSON request body"},
		{"unread stdin", []string{"--ledger", "books", "transactions", "create", "--data", "-"}, "", "already-read JSON request body"},
		{"missing required body", []string{"--ledger", "books", "transactions", "create"}, "", "--data is required"},
		{"empty explicit optional body", []string{"--ledger", "books", "transactions", "revert", "42", "--data", ""}, "", "valid JSON"},
		{"unconfirmed delete", []string{"delete", "books"}, "", "requires --confirm"},
		{"invalid consistency", []string{"--ledger", "books", "stats", "--consistency", "eventual"}, "", "consistency must be"},
		{"invalid idempotency header", []string{"create", "books", "--idempotency-key", "a\nb"}, "", "without newlines"},
		{"invalid query type", []string{"--ledger", "books", "accounts", "list", "--page-size", "-1"}, "", "invalid --page-size"},
		{"invalid after ID", []string{"--ledger", "books", "transactions", "list", "--after", "-1"}, "", "after must be"},
		{"invalid date", []string{"--ledger", "books", "transactions", "list", "--start-date", "yesterday"}, "", "startDate must be"},
		{"invalid inspection mode", []string{"--ledger", "books", "indexes", "inspect", "index", "--mode", "bad"}, "", "mode must be"},
		{"invalid inspection page size", []string{"--ledger", "books", "indexes", "inspect", "index", "--page-size", "0"}, "", "index page-size must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			p := NewWithExecutor("", func(context.Context, pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
				calls++

				return pluginsdk.ExecuteResponse{}, nil
			})
			manifest, err := p.GetManifest(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Execute(t.Context(), testRequest(manifest, tc.tokens, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) || calls != 0 {
				t.Fatalf("want %q before dispatch, calls=%d err=%v", tc.want, calls, err)
			}
		})
	}
}

func TestExecutorManifestIndependentOfTransport(t *testing.T) {
	t.Parallel()
	p := NewWithExecutor("3.0.1", nil)
	manifest, err := p.GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	httpManifest, err := NewVersion(nil, "3.0.1").GetManifest(t.Context())
	if err != nil || !reflect.DeepEqual(manifest, httpManifest) {
		t.Fatalf("transport changed command or form definitions: err=%v", err)
	}
	manifest.Root.Flags[0].Name = "corrupted"
	fresh, err := p.GetManifest(t.Context())
	if err != nil || fresh.Root.Flags[0].Name != "ledger" {
		t.Fatal("caller changed a subsequent manifest")
	}
	if _, err := p.Execute(t.Context(), pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "list"}}); err == nil || !strings.Contains(err.Error(), "requires an executor") {
		t.Fatalf("nil executor error = %v", err)
	}
	defaults, err := NewWithExecutor("", nil).GetManifest(t.Context())
	if err != nil || defaults.Version != defaultServiceVersion {
		t.Fatalf("default service version = %q, err=%v", defaults.Version, err)
	}
}

func TestExecutorCancellation(t *testing.T) {
	t.Parallel()
	t.Run("before dispatch", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		calls := 0
		p := NewWithExecutor("", func(context.Context, pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
			calls++

			return pluginsdk.ExecuteResponse{}, nil
		})
		_, err := p.Execute(ctx, pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "list"}})
		if !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("canceled request dispatched: calls=%d err=%v", calls, err)
		}
	})
	t.Run("in flight preserves partial data", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		started := make(chan struct{})
		type outcome struct {
			response pluginsdk.ExecuteResponse
			err      error
		}
		done := make(chan outcome, 1)
		const partial = `{"data":{"id":9007199254740993}}`
		p := NewWithExecutor("", func(ctx context.Context, _ pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
			close(started)
			<-ctx.Done()

			return pluginsdk.ExecuteResponse{Data: json.RawMessage(partial)}, ctx.Err()
		})
		go func() {
			response, err := p.Execute(ctx, pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "list"}})
			done <- outcome{response: response, err: err}
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("executor did not start")
		}
		cancel()
		select {
		case result := <-done:
			if !errors.Is(result.err, context.Canceled) || string(result.response.Data) != partial {
				t.Fatalf("in-flight cancellation or partial data lost: data=%s err=%v", result.response.Data, result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("executor did not stop after cancellation")
		}
	})
}

func TestHTTPExecutorRetainsEndpointValidationOrder(t *testing.T) {
	t.Parallel()
	for _, canceled := range []bool{false, true} {
		name := "active context"
		if canceled {
			name = "canceled context"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if canceled {
				cancel()
			}
			req := pluginsdk.ExecuteRequest{
				CommandPath: []string{"ledger", "transactions", "create"},
				Flags:       map[string]string{"ledger": "books", "data": "@unread.json"},
				Endpoint:    "invalid",
			}
			// Existing HTTP execution validates the endpoint before preparing an
			// unread body or observing cancellation in the HTTP request.
			for _, p := range []pluginsdk.Plugin{New(nil), NewVersion(nil, "3.0.1")} {
				_, err := p.Execute(ctx, req)
				if err == nil || !strings.Contains(err.Error(), "endpoint must be an absolute HTTP(S) URL") {
					t.Fatalf("HTTP validation order changed: %v", err)
				}
			}
		})
	}
}
