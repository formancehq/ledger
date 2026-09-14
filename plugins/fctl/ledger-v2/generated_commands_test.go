package ledgerv2

import (
	"context"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func TestGeneratedAdapterExecutesEveryPrimaryCommandFamily(t *testing.T) {
	t.Parallel()
	tests := []struct {
		id    string
		args  []string
		flags []sdk.FlagOccurrence
	}{
		{id: "ledger.v2.list"},
		{id: "ledger.v2.create", args: []string{"primary"}, flags: []sdk.FlagOccurrence{{Name: "metadata", Value: "region=eu"}, {Name: "features", Value: "experimental=true"}}},
		{id: "ledger.v2.set-metadata", args: []string{"primary", "region=eu"}},
		{id: "ledger.v2.delete-metadata", args: []string{"primary", "region"}},
		{id: "ledger.v2.export", flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.stats", flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.send", args: []string{"merchant", "1250", "USD/2"}, flags: append(ledgerFlagOccurrence(), sdk.FlagOccurrence{Name: "source", Value: "users:001"}, sdk.FlagOccurrence{Name: "metadata", Value: "region=eu"})},
		{id: "ledger.v2.accounts.list", flags: append(ledgerFlagOccurrence(), sdk.FlagOccurrence{Name: "metadata", Value: "tier=gold"})},
		{id: "ledger.v2.accounts.show", args: []string{"users:001"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.accounts.set-metadata", args: []string{"users:001", "tier=gold"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.accounts.delete-metadata", args: []string{"users:001", "tier"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.transactions.list", flags: append(ledgerFlagOccurrence(), sdk.FlagOccurrence{Name: "account", Value: "users:001"}, sdk.FlagOccurrence{Name: "start", Value: "2026-09-12T00:00:00Z"})},
		{id: "ledger.v2.transactions.show", args: []string{"42"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.transactions.set-metadata", args: []string{"42", "region=eu"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.transactions.delete-metadata", args: []string{"42", "region"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.transactions.revert", args: []string{"42"}, flags: append(ledgerFlagOccurrence(), sdk.FlagOccurrence{Name: "force", Value: "true"}, sdk.FlagOccurrence{Name: "at-effective-date", Value: "true"})},
		{id: "ledger.v2.volumes.list", flags: append(ledgerFlagOccurrence(), sdk.FlagOccurrence{Name: "address", Value: "users:001"}, sdk.FlagOccurrence{Name: "insertion-date", Value: "true"}, sdk.FlagOccurrence{Name: "start-time", Value: "2026-09-12T00:00:00Z"}, sdk.FlagOccurrence{Name: "end-time", Value: "2026-09-13T00:00:00Z"})},
		{id: "ledger.v2.schemas.list", flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.schemas.get", args: []string{"v1"}, flags: ledgerFlagOccurrence()},
		{id: "ledger.v2.schemas.insert", args: []string{"v1", `{"chart":{}}`}, flags: ledgerFlagOccurrence()},
	}
	for _, test := range tests {
		test := test
		t.Run(test.id, func(t *testing.T) {
			host := sdk.NewMemoryHost(generatedSuccessResponse)
			if err := (Plugin{}).Execute(context.Background(), execution(test.id, test.args, test.flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if events := host.Events(); len(events) != 1 || events[0].Result == nil || events[0].Result.OperationID != test.id {
				t.Fatalf("events = %#v", events)
			}
		})
	}
}

func TestGeneratedAdapterForwardsEveryDeclaredIdempotencyKey(t *testing.T) {
	tests := []struct {
		id   string
		args []string
	}{
		{id: "ledger.v2.accounts.delete-metadata", args: []string{"users:001", "tier"}},
		{id: "ledger.v2.accounts.set-metadata", args: []string{"users:001", "tier=gold"}},
		{id: "ledger.v2.schemas.insert", args: []string{"v1", `{"chart":{}}`}},
		{id: "ledger.v2.send", args: []string{"merchant", "1250", "USD/2"}},
		{id: "ledger.v2.transactions.delete-metadata", args: []string{"42", "region"}},
		{id: "ledger.v2.transactions.num", args: []string{"artifact"}},
		{id: "ledger.v2.transactions.revert", args: []string{"42"}},
		{id: "ledger.v2.transactions.set-metadata", args: []string{"42", "region=eu"}},
	}
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			memory := sdk.NewMemoryHost(generatedSuccessResponse)
			var host sdk.Host = memory
			if test.id == "ledger.v2.transactions.num" {
				host = &artifactHost{MemoryHost: memory, chunks: []sdk.InputArtifactChunk{{Bytes: []byte("send [USD 1]"), Final: true}}}
			}
			flags := []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}, {Name: "idempotency-key", Value: "idem-42"}}
			if err := (Plugin{}).Execute(context.Background(), execution(test.id, test.args, flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			requests := memory.Requests()
			if len(requests) != 1 || requests[0].HTTP == nil {
				t.Fatalf("requests = %#v", requests)
			}
			if got := requestHeader(requests[0].HTTP.Headers, "Idempotency-Key"); got != "idem-42" {
				t.Fatalf("Idempotency-Key = %q in %#v", got, requests[0].HTTP.Headers)
			}
		})
	}
}

func requestHeader(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) != 0 {
			return values[0]
		}
	}
	return ""
}

func TestGeneratedAdapterRejectsIdempotencyKeyWhenInventoryDoesNotDeclareIt(t *testing.T) {
	host := sdk.NewMemoryHost(generatedSuccessResponse)
	err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.delete-metadata", []string{"primary", "key"}, sdk.FlagOccurrence{Name: "idempotency-key", Value: "ignored"}), host)
	if err == nil {
		t.Fatal("undeclared idempotency-key was silently ignored")
	}
	if len(host.Requests()) != 0 {
		t.Fatal("invalid flag reached product traffic")
	}
}

func ledgerFlagOccurrence() []sdk.FlagOccurrence {
	return []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}}
}

func generatedSuccessResponse(_ context.Context, request sdk.Request) (sdk.Responses, error) {
	status := int32(200)
	mediaType := mediaTypeJSON
	var body []byte
	switch request.Operation {
	case "v2ListLedgers", "v2ListAccounts", "v2ListTransactions", "v2GetVolumesWithBalances", "v2ListSchemas":
		body = []byte(`{"cursor":{"data":[],"pageSize":15,"hasMore":false}}`)
	case "v2ReadStats":
		body = []byte(`{"data":{"accounts":1,"transactions":2}}`)
	case "v2GetAccount":
		body = []byte(`{"data":{"address":"users:001","metadata":{}}}`)
	case "v2CreateTransaction", "v2GetTransaction", "v2RevertTransaction":
		body = []byte(`{"data":{"timestamp":"2026-09-12T00:00:00Z","postings":[],"metadata":{},"id":42,"reverted":false}}`)
		if request.Operation == "v2RevertTransaction" {
			status = 201
		}
	case "v2GetSchema":
		body = []byte(`{"data":{"version":"v1","createdAt":"2026-09-12T00:00:00Z","chart":{}}}`)
	case "v2ExportLogs":
		mediaType = mediaTypeOctetStream
		body = []byte("logs")
	default:
		status = 204
	}
	return sdk.NewResponseStream(sdk.Response{Status: status, ContentType: mediaType, Body: body}), nil
}
