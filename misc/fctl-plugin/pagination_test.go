package ledger

import (
	"context"
	"encoding/base64"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestUnsupportedPaginationRejectedBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"list", "--after", "books"},
		{"--ledger", "books", "indexes", "inspect", "metadata:TARGET_TYPE_ACCOUNT:key", "--after", "42"},
	} {
		out, requests, err := execute(t, args, "", envelope, 200)
		if err == nil || len(requests) != 0 || out != "" {
			t.Fatalf("unsupported flags accepted: %v; requests=%v out=%s err=%v", args, requests, out, err)
		}
	}
}

func TestAfterIDValidationBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, collection := range []string{"transactions", "logs"} {
		for _, after := range []string{"opaque/+=", "-1", "18446744073709551616"} {
			out, requests, err := execute(t, []string{"--ledger", "books", collection, "list", "--after", after}, "", envelope, 200)
			if err == nil || len(requests) != 0 || out != "" {
				t.Fatalf("invalid %s ID %q reached server: requests=%v err=%v", collection, after, requests, err)
			}
		}
	}
}

func TestLedgerListReturnsOnePageWithDefaultSize(t *testing.T) {
	t.Parallel()
	response := `{"data":[{"name":"books"}],"next":"next-page","hasMore":true}`
	out, requests, err := execute(t, []string{"list"}, "", response, 200)
	if err != nil || out != response || len(requests) != 1 {
		t.Fatalf("list page: out=%s requests=%v err=%v", out, requests, err)
	}
	if requests[0].query.Get("pageSize") != "100" || len(requests[0].query) != 1 || requests[0].path != "/gateway/ledger/v3/" {
		t.Fatalf("list pagination: %+v", requests[0])
	}
}

func TestCursorQueryAndAfterCompatibility(t *testing.T) {
	t.Parallel()
	for _, collection := range []string{"", "accounts", "transactions", "logs"} {
		for _, backward := range []bool{false, true} {
			t.Run(collection+map[bool]string{false: "/next", true: "/previous"}[backward], func(t *testing.T) {
				t.Parallel()
				key := "18446744073709551615"
				if collection == "accounts" || collection == "" {
					key = "users:next/+="
				}
				cursorJSON := `{"key":"` + key + `"}`
				if backward {
					cursorJSON = `{"key":"` + key + `","back":true}`
				}
				token := base64.RawURLEncoding.EncodeToString([]byte(cursorJSON))
				args := []string{"--consistency", "stale"}
				if collection != "" {
					args = append(args, "--ledger", "books", collection)
				}
				args = append(args, "list", "--page-size", "0", "--reverse=false", "--cursor", token)
				_, requests, err := execute(t, args, "", envelope, 200)
				if err != nil || len(requests) != 1 {
					t.Fatalf("cursor query: requests=%v err=%v", requests, err)
				}
				checkQuery(t, requests[0].query, map[string]string{"cursor": token, "pageSize": "0", "reverse": "false"})
				if requests[0].query.Has("after") || requests[0].header.Get("X-Consistency") != "stale" {
					t.Fatalf("cursor or consistency lost: %+v", requests[0])
				}
				if collection == "" || backward {
					return
				}
				args = append(args[:len(args)-2], "--after", key)
				_, requests, err = execute(t, args, "", envelope, 200)
				if err != nil || len(requests) != 1 || requests[0].query.Get("cursor") != token || requests[0].query.Has("after") {
					t.Fatalf("after alias did not use canonical cursor: requests=%v err=%v", requests, err)
				}
			})
		}
	}
}

func TestPaginationNormalizationForAlternateExecutor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		collection, after, key string
		changed                map[string]bool
	}{
		{"accounts", "users:42/+=", "users:42/+=", map[string]bool{"after": true}},
		{"transactions", "00042", "42", map[string]bool{"after": true, "reverse": true}},
		{"logs", "18446744073709551615", "18446744073709551615", nil},
	} {
		t.Run(tc.collection, func(t *testing.T) {
			t.Parallel()
			token := base64.RawURLEncoding.EncodeToString([]byte(`{"key":"` + tc.key + `"}`))
			calls := 0
			p := NewWithExecutor("", func(_ context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
				calls++
				_, afterFlag := req.Flags["after"]
				_, afterChanged := req.ChangedFlags["after"]
				if req.Flags["cursor"] != token || !req.ChangedFlags["cursor"] || afterFlag || afterChanged {
					t.Fatalf("executor received an unnormalized alias: %#v", req)
				}
				if req.ChangedFlags["reverse"] != tc.changed["reverse"] {
					t.Fatalf("pagination normalization lost another changed flag: %#v", req)
				}

				return pluginsdk.ExecuteResponse{}, nil
			})
			req := pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", tc.collection, "list"}, Flags: map[string]string{"ledger": "books", "after": tc.after}, ChangedFlags: tc.changed}
			beforeFlags, beforeChanged := maps.Clone(req.Flags), maps.Clone(req.ChangedFlags)
			if _, err := p.Execute(t.Context(), req); err != nil || calls != 1 {
				t.Fatalf("normalized execute: calls=%d err=%v", calls, err)
			}
			if !reflect.DeepEqual(req.Flags, beforeFlags) || !reflect.DeepEqual(req.ChangedFlags, beforeChanged) {
				t.Fatal("pagination normalization mutated caller maps")
			}
		})
	}
}

func TestInvalidCursorRejectedBeforeExecutors(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{`null`, `[]`, `{"key":null}`, `{"key":42}`, `{"key":"-1"}`, `{"key":"18446744073709551616"}`, `{"back":"true"}`, `{"back":null}`, `{"other":true}`, `{} {}`} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			token := base64.RawURLEncoding.EncodeToString([]byte(raw))
			assertPaginationRejected(t, []string{"--ledger", "books", "transactions", "list", "--cursor", token}, "invalid --cursor")
		})
	}
	assertPaginationRejected(t, []string{"--ledger", "books", "transactions", "list", "--cursor", "not/base64"}, "invalid --cursor")
	for _, collection := range []string{"accounts", "transactions", "logs"} {
		assertPaginationRejected(t, []string{"--ledger", "books", collection, "list", "--cursor", "eyJrZXkiOiI0MiJ9", "--after", "42"}, "cannot be used together")
	}
}

func assertPaginationRejected(t *testing.T, tokens []string, want string) {
	t.Helper()
	out, requests, err := execute(t, tokens, "", envelope, 200)
	if err == nil || !strings.Contains(err.Error(), want) || len(requests) != 0 || out != "" {
		t.Fatalf("HTTP: want %q before dispatch; requests=%v out=%s err=%v", want, requests, out, err)
	}
	calls := 0
	p := NewWithExecutor("", func(context.Context, pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
		calls++

		return pluginsdk.ExecuteResponse{}, nil
	})
	manifest, err := p.GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(t.Context(), testRequest(manifest, tokens, "")); err == nil || !strings.Contains(err.Error(), want) || calls != 0 {
		t.Fatalf("alternate executor: want %q before dispatch; calls=%d err=%v", want, calls, err)
	}
}
