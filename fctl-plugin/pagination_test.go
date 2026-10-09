package ledger

import (
	"testing"
)

func TestUnsupportedPaginationRejectedBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"list", "--page-size", "10"},
		{"list", "--reverse=true"},
		{"list", "--cursor", "next"},
		{"list", "--after", "books"},
		{"--ledger", "books", "logs", "list", "--reverse=true"},
		{"--ledger", "books", "accounts", "list", "--cursor", "users:42"},
		{"--ledger", "books", "transactions", "list", "--cursor", "42"},
		{"--ledger", "books", "logs", "list", "--cursor", "42"},
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

func TestLedgerListReturnsAllWithoutPaginationQuery(t *testing.T) {
	t.Parallel()
	response := `{"data":[{"name":"books"},{"name":"other"}]}`
	out, requests, err := execute(t, []string{"list"}, "", response, 200)
	if err != nil || out != response || len(requests) != 1 {
		t.Fatalf("list all: out=%s requests=%v err=%v", out, requests, err)
	}
	if len(requests[0].query) != 0 || requests[0].path != "/gateway/ledger/v3/" {
		t.Fatalf("list all sent pagination: %+v", requests[0])
	}
}
