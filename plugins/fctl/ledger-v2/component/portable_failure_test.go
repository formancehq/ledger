package component

import (
	"bytes"
	"testing"

	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	portablecomponent "github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable/component"
	ledgerv2 "github.com/formancehq/ledger/plugins/fctl/ledger-v2"
	"google.golang.org/protobuf/proto"
)

// portableFailure drives one command through the real portable lifecycle — the
// boundary the delivered component actually ships — and returns the terminal
// failure the host would observe for a product response with the given status.
func portableFailure(t *testing.T, status int32, body []byte) *pb.Failure {
	t.Helper()

	adapter, err := portablecomponent.NewCommand(ledgerv2.Plugin{}, Descriptor())
	if err != nil {
		t.Fatalf("NewCommand() error = %v", err)
	}
	const executionID = "portable-failure"
	t.Cleanup(func() { adapter.Close(executionID) })

	started := adapter.Start(executionID, portableInput(t, executionID, pb.MessageKind_MESSAGE_KIND_START_EXECUTION, &pb.StartPayload{
		Start: &pb.StartPayload_Command{Command: &pb.CommandStart{
			CommandId:       "ledger.v2.stats",
			Flags:           []*pb.FlagOccurrence{{Name: "ledger", Value: "primary"}},
			Target:          &pb.TargetCoordinates{OrganizationId: "org", StackId: "stack"},
			ServiceVersions: []*pb.ServiceVersion{{Service: pb.Service_SERVICE_LEDGER, Version: "2.0.0", Major: 2}},
			Continuation:    &pb.ContinuationControl{Mode: pb.ContinuationMode_CONTINUATION_MODE_SINGLE_PAGE},
		}},
	}))
	if len(started) != 1 {
		t.Fatalf("Start frames = %d, want one host request", len(started))
	}
	var request pb.HostRequestPayload
	portableDecode(t, started[0], pb.MessageKind_MESSAGE_KIND_HOST_REQUEST, &request)

	resumed := adapter.Resume(executionID, portableInput(t, executionID, pb.MessageKind_MESSAGE_KIND_HOST_RESPONSE, &pb.HostResponsePayload{
		CorrelationId: request.GetCorrelationId(),
		Response: &pb.HostResponsePayload_Product{Product: &pb.ProductResponse{
			Status: status, ContentType: "application/json", Body: body,
		}},
	}))
	if len(resumed) != 1 {
		t.Fatalf("Resume frames = %d, want one termination and no partial result", len(resumed))
	}
	var terminal pb.TerminationPayload
	portableDecode(t, resumed[0], pb.MessageKind_MESSAGE_KIND_TERMINATION, &terminal)
	failure := terminal.GetFailure()
	if failure == nil {
		t.Fatalf("termination = %#v, want a failure", &terminal)
	}
	return failure
}

func portableInput(t *testing.T, executionID string, kind pb.MessageKind, payload proto.Message) []byte {
	t.Helper()
	encodedPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb.PluginEnvelope{
		ProtocolMajor: pb.ProtocolMajor,
		FacetKind:     pb.FacetKind_FACET_KIND_COMMAND_PROVIDER,
		MessageKind:   kind,
		ExecutionId:   executionID,
		Payload:       encodedPayload,
	})
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return encoded
}

func portableDecode(t *testing.T, encoded []byte, kind pb.MessageKind, payload proto.Message) {
	t.Helper()
	var envelope pb.PluginEnvelope
	if err := proto.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.GetMessageKind() != kind {
		t.Fatalf("message kind = %s, want %s", envelope.GetMessageKind(), kind)
	}
	if err := proto.Unmarshal(envelope.GetPayload(), payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
}

// The adapter's typed product_http_error detail is real, but it is
// adapter-local: it exists for a host that embeds this plugin in process
// through sdk.Host. The delivered artifact is a portable component, and
// portable.Frame carries only FailureCode, so component/command.go rebuilds the
// wire failure as sdk.Failure{Code: frame.FailureCode}. The originating status
// and the retryable flag never reach a component host, even though the wire
// codec could carry them.
//
// This test drives the real lifecycle rather than asserting the SDK's source,
// so the day the pump grows a detail field the assertion fails and the claim in
// README and adapter_v2_edges_test.go gets revisited instead of silently
// becoming stale.
func TestPortableBoundaryCarriesOnlyTheFailureCode(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int32
		code   sdk.FailureCode
	}{
		{name: "client error", status: 404, code: sdk.FailureProductHTTPError},
		{name: "retryable server error", status: 503, code: sdk.FailureProductHTTPError},
		{name: "out of range", status: 600, code: sdk.FailureProductResponseFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := portableFailure(t, test.status, []byte(`{"errorCode":"INSUFFICIENT_FUND","errorMessage":"upstream unavailable"}`))

			if got, want := failure.GetCode(), string(test.code); got != want {
				t.Fatalf("portable failure code = %q, want %q", got, want)
			}
			if failure.GetRetryable() {
				t.Errorf("portable failure carries retryable=true; portable.Frame has no field for it")
			}
			if len(failure.GetDetails()) != 0 {
				t.Errorf("portable failure carries details %q; portable.Frame has no field for them", failure.GetDetails())
			}
			if failure.GetMessage() != "" {
				t.Errorf("portable failure carries message %q; portable.Frame drops it", failure.GetMessage())
			}
		})
	}
}

// Whatever the portable boundary does carry, it must never carry the product's
// own error body. producthttp short-circuits an in-range non-2xx before the
// body is decoded, so `errorCode` and `errorMessage` are discarded at the
// adapter; this pins that no later stage reintroduces them.
func TestPortableBoundaryLeaksNoProductErrorBody(t *testing.T) {
	failure := portableFailure(t, 409, []byte(`{"errorCode":"INSUFFICIENT_FUND","errorMessage":"account users:001 lacks 250 USD","details":"users:001"}`))

	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(failure)
	if err != nil {
		t.Fatalf("encode failure: %v", err)
	}
	for _, leaked := range []string{"INSUFFICIENT_FUND", "account users:001", "users:001", "250 USD"} {
		if bytes.Contains(encoded, []byte(leaked)) {
			t.Errorf("portable failure leaks the product error body fragment %q", leaked)
		}
	}
}
