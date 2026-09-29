package ledgerv2

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func execution(commandID string, arguments []string, flags ...sdk.FlagOccurrence) sdk.ExecuteRequest {
	return sdk.ExecuteRequest{
		CommandID:       commandID,
		Arguments:       arguments,
		Flags:           flags,
		Target:          sdk.TargetSelection{OrganizationID: "org", StackID: "stack"},
		ServiceVersions: []sdk.ServiceVersion{{Service: sdk.ServiceLedger, Version: "2.0.0", Major: 2}},
		Continuation:    sdk.SinglePageContinuationControl(),
	}
}

func TestExecuteV2MapsAccountShowAndEmitsTheUnwrappedResource(t *testing.T) {
	t.Parallel()

	host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		if request.Operation != "v2GetAccount" {
			t.Fatalf("operation = %q, want v2GetAccount", request.Operation)
		}
		if request.HTTP == nil || request.HTTP.Method != "GET" || request.HTTP.Path != "/v2/main/accounts/users%3A001" {
			t.Fatalf("HTTP request = %#v", request.HTTP)
		}
		return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: "application/json", Body: []byte(`{"data":{"address":"users:001","metadata":{}}}`)}), nil
	})

	err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.accounts.show", []string{"users:001"}, sdk.FlagOccurrence{Name: "ledger", Value: "main"}), host)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	events := host.Events()
	if len(events) != 1 || events[0].Result == nil {
		t.Fatalf("events = %#v, want one result", events)
	}
	if got, want := events[0].Result.OperationID, "ledger.v2.accounts.show"; got != want {
		t.Fatalf("result operation ID = %q, want %q", got, want)
	}
	if got, want := string(events[0].Result.Data), `{"address":"users:001","metadata":{}}`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestExecuteV2RejectsAResponseThatViolatesTheGeneratedDTO(t *testing.T) {
	t.Parallel()

	host := sdk.NewMemoryHost(func(_ context.Context, _ sdk.Request) (sdk.Responses, error) {
		return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: "application/json", Body: []byte(`{"data":{"address":"users:001"}}`)}), nil
	})
	err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.accounts.show", []string{"users:001"}, sdk.FlagOccurrence{Name: "ledger", Value: "main"}), host)
	if err == nil {
		t.Fatal("Execute() accepted a response missing generated V2Account.metadata")
	}
	if len(host.Events()) != 0 {
		t.Fatal("invalid generated response emitted output")
	}
}

func TestGeneratedClientIgnoresAmbientProductCredentials(t *testing.T) {
	t.Setenv("formance_client_id", "ambient-client")
	t.Setenv("formance_client_secret", "ambient-secret")

	host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		if request.Operation != "v2GetAccount" {
			t.Fatalf("operation = %q, want v2GetAccount", request.Operation)
		}
		return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: "application/json", Body: []byte(`{"data":{"address":"users:001","metadata":{}}}`)}), nil
	})
	err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.accounts.show", []string{"users:001"}, sdk.FlagOccurrence{Name: "ledger", Value: "main"}), host)
	if err != nil {
		t.Fatalf("Execute() used ambient generated-client auth: %v", err)
	}
	if got := len(host.Requests()); got != 1 {
		t.Fatalf("host requests = %d, want one product request", got)
	}
}

func TestExecuteV2BuildsAccountMetadataMutation(t *testing.T) {
	t.Parallel()

	host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		if request.HTTP == nil || request.HTTP.Path != "/v2/main/accounts/users%3A001/metadata" {
			t.Fatalf("HTTP request = %#v", request.HTTP)
		}
		var body map[string]string
		if err := json.Unmarshal(request.HTTP.Body, &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if !reflect.DeepEqual(body, map[string]string{"region": "eu", "tier": "gold"}) {
			t.Fatalf("body = %#v", body)
		}
		return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: "application/json", Body: nil}), nil
	})

	err := (Plugin{}).Execute(context.Background(), execution(
		"ledger.v2.accounts.set-metadata",
		[]string{"users:001", "region=eu", "tier=gold"},
		sdk.FlagOccurrence{Name: "ledger", Value: "main"},
	), host)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if events := host.Events(); len(events) != 1 || events[0].Result == nil || events[0].Result.Shape != sdk.ResultObject || string(events[0].Result.Data) != `{}` {
		t.Fatalf("events = %#v, want one empty object result", events)
	}
}

func TestExecuteV2TraversesOpaqueCursorsWhenAllPagesIsRequested(t *testing.T) {
	t.Parallel()

	host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		body := `{"cursor":{"data":[{"name":"one","addedAt":"2026-09-12T00:00:00Z","bucket":"default"}],"hasMore":true,"next":"opaque-2","pageSize":1}}`
		if reflect.DeepEqual(request.HTTP.Query["cursor"], []string{"opaque-2"}) {
			if len(request.HTTP.Query) != 1 || len(request.HTTP.Body) != 0 {
				t.Fatalf("second page request = query %#v body %q, want cursor only", request.HTTP.Query, request.HTTP.Body)
			}
			body = `{"cursor":{"data":[{"name":"two","addedAt":"2026-09-12T00:00:00Z","bucket":"default"}],"hasMore":false,"pageSize":1}}`
		}
		return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: "application/json", Body: []byte(body)}), nil
	})
	request := execution("ledger.v2.list", nil)
	request.Continuation = sdk.AllPagesContinuationControl()

	if err := (Plugin{}).Execute(context.Background(), request, host); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(host.Requests()); got != 2 {
		t.Fatalf("Host.Request calls = %d, want 2", got)
	}
	events := host.Events()
	if got, want := events[0].Result.OperationID, "ledger.v2.list"; got != want {
		t.Fatalf("result operation ID = %q, want %q", got, want)
	}
	if got, want := string(events[0].Result.Data), `[{"addedAt":"2026-09-12T00:00:00Z","bucket":"default","name":"one"},{"addedAt":"2026-09-12T00:00:00Z","bucket":"default","name":"two"}]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}
