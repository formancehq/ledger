package ledgerv3

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The signed Apply path is split across two repositories, so the contract
// between them is stated here in the product's own terms and asserted from
// this side.
//
// The plugin never sees a signature. RFC 0009 forbids exposing one to a
// product plugin and RFC 0012's payload schema enforces it, so the plugin's
// only obligation is to put the exact serialized ApplyBatch in the unsigned
// variant of an ordinary Apply request and never to send a signed variant
// itself. When a signer is activated, the fctl host lifts those exact bytes
// off the wire, signs them, and substitutes
// `SignedApplyBatch{key_id, signature, payload}` for the unsigned variant.
//
// These are the field numbers that substitution depends on. A change to them
// in misc/proto is a breaking change to the host, which is why they are
// asserted against the pinned descriptors below rather than trusted.
const (
	applyRequestUnsignedField     = protowire.Number(1)
	applyRequestSignedField       = protowire.Number(2)
	applyRequestSkipResponseField = protowire.Number(4)

	signedApplyBatchKeyIDField     = protowire.Number(1)
	signedApplyBatchSignatureField = protowire.Number(2)
	signedApplyBatchPayloadField   = protowire.Number(3)
)

// The logical fctl payload identifier RFC 0009 binds to one exact protobuf
// message, and the one method the capability covers.
const (
	signedApplyBatchFullName = protoreflect.FullName("ledger.ApplyBatch")
	signedApplyFullMethod    = "/ledger.BucketService/Apply"
)

// The descriptor's signing declaration is only meaningful if it names the
// message and method the host will actually act on. This is the reconciliation
// RFC 0009 requires before signed execution may be claimed.
func TestSigningDeclarationIsBoundToThePinnedApplyBatchAndMethod(t *testing.T) {
	t.Parallel()

	if signingPayloadType != "formance.ledger.v3.ApplyBatch" {
		t.Fatalf("signing payload type = %q", signingPayloadType)
	}
	batch := servicepb.File_bucket_proto.Messages().ByName("ApplyBatch")
	if batch == nil || batch.FullName() != signedApplyBatchFullName {
		t.Fatalf("pinned ApplyBatch full name = %v, want %v", batch, signedApplyBatchFullName)
	}
	if bucketFullMethod("Apply") != signedApplyFullMethod {
		t.Fatalf("apply method = %q, want %q", bucketFullMethod("Apply"), signedApplyFullMethod)
	}

	signing := 0
	for _, command := range (Plugin{}).Commands() {
		declaresApply := false
		for _, operation := range command.Operations {
			if operation.GRPC != nil && operation.GRPC.FullMethod == signedApplyFullMethod {
				declaresApply = true
			}
		}
		if command.RequestSigning == nil {
			if declaresApply {
				t.Fatalf("command %q declares Apply without request signing", command.ID)
			}
			continue
		}
		if !declaresApply {
			t.Fatalf("command %q declares request signing without Apply", command.ID)
		}
		if command.RequestSigning.Capability != sdk.CapabilitySignLedgerApplyBatch ||
			command.RequestSigning.ProductMajor != productMajor ||
			command.RequestSigning.PayloadType != signingPayloadType {
			t.Fatalf("command %q signing = %#v", command.ID, command.RequestSigning)
		}
		signing++
	}
	if signing == 0 {
		t.Fatal("no command declares request signing")
	}
}

// The field numbers the host substitution depends on are the ones the pinned
// Ledger descriptors actually declare.
func TestSignedApplyWireContractMatchesThePinnedDescriptors(t *testing.T) {
	t.Parallel()

	request := servicepb.File_bucket_proto.Messages().ByName("ApplyRequest")
	if request == nil {
		t.Fatal("pinned ApplyRequest descriptor is unavailable")
	}
	for name, want := range map[protoreflect.Name]protowire.Number{
		"unsigned":      applyRequestUnsignedField,
		"signed":        applyRequestSignedField,
		"skip_response": applyRequestSkipResponseField,
	} {
		field := request.Fields().ByName(name)
		if field == nil || protowire.Number(field.Number()) != want {
			t.Fatalf("ApplyRequest.%s = %v, want field %d", name, field, want)
		}
	}
	envelope := signaturepb.File_signature_proto.Messages().ByName("SignedApplyBatch")
	if envelope == nil {
		t.Fatal("pinned SignedApplyBatch descriptor is unavailable")
	}
	for name, want := range map[protoreflect.Name]protowire.Number{
		"key_id":    signedApplyBatchKeyIDField,
		"signature": signedApplyBatchSignatureField,
		"payload":   signedApplyBatchPayloadField,
	} {
		field := envelope.Fields().ByName(name)
		if field == nil || protowire.Number(field.Number()) != want {
			t.Fatalf("SignedApplyBatch.%s = %v, want field %d", name, field, want)
		}
	}
}

// Every Apply the plugin issues carries exactly one unsigned batch and no
// signature material. A plugin that signed its own request would be refused by
// the host and would violate the key-isolation boundary.
func TestEveryApplyRequestThePluginIssuesIsUnsignedAndLiftable(t *testing.T) {
	t.Parallel()

	command, request, decoded := decodedMutation(t, "ledger.v3.accounts.set-metadata",
		[]string{"main", "users:42"},
		[]sdk.FlagOccurrence{
			{Name: flagMetadata, Value: "category=premium"},
			{Name: flagMetadata, Value: "enabled=true"},
			{Name: flagIdempotencyKey, Value: "batch-1"},
		})

	var message []byte
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		if got.GRPC == nil || got.GRPC.FullMethod != signedApplyFullMethod {
			t.Fatalf("host request = %#v", got)
		}
		message = append([]byte(nil), got.GRPC.Message...)
		return sdk.NewResponseStream(mutationProtoResponse(t, &servicepb.ApplyResponse{})), nil
	})

	handled, err := executeV3AccountMutations(context.Background(), request, decoded, command, host)
	if err != nil || !handled {
		t.Fatalf("executeV3AccountMutations() = %v, %v", handled, err)
	}

	payload, found := liftUnsignedApplyBatch(t, message)
	if found != 1 {
		t.Fatalf("unsigned variants on the wire = %d, want exactly one", found)
	}
	if len(payload) == 0 {
		t.Fatal("the unsigned variant carries no batch")
	}
	if signedVariants(t, message) != 0 {
		t.Fatal("the plugin emitted a signed variant itself")
	}
}

// The host's substitution, performed here exactly as the host performs it,
// produces a request the Ledger server's own verification chain accepts and
// whose batch is the one the plugin built. This is the end the two
// repositories have to agree on.
func TestHostSubstitutionOverThePluginsBytesSatisfiesTheServerChain(t *testing.T) {
	t.Parallel()

	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	command, request, decoded := decodedMutation(t, "ledger.v3.accounts.set-metadata",
		[]string{"main", "users:42"},
		[]sdk.FlagOccurrence{
			{Name: flagMetadata, Value: "category=premium"},
			{Name: flagMetadata, Value: "enabled=true"},
			{Name: flagMetadata, Value: "tier=gold"},
			{Name: flagMetadata, Value: "region=eu"},
			{Name: flagMetadata, Value: "owner=ops"},
			{Name: flagIdempotencyKey, Value: "batch-1"},
		})

	var message []byte
	host := sdk.NewMemoryHost(func(_ context.Context, got sdk.Request) (sdk.Responses, error) {
		message = append([]byte(nil), got.GRPC.Message...)
		return sdk.NewResponseStream(mutationProtoResponse(t, &servicepb.ApplyResponse{})), nil
	})
	if handled, err := executeV3AccountMutations(context.Background(), request, decoded, command, host); err != nil || !handled {
		t.Fatalf("executeV3AccountMutations() = %v, %v", handled, err)
	}

	payload, _ := liftUnsignedApplyBatch(t, message)
	substituted := substituteSignedApplyBatch(t, message, &signaturepb.SignedApplyBatch{
		KeyId:     "key-1",
		Signature: ed25519.Sign(private, payload),
		Payload:   payload,
	})

	var wire servicepb.ApplyRequest
	if err := proto.Unmarshal(substituted, &wire); err != nil {
		t.Fatalf("the substituted request is not an ApplyRequest: %v", err)
	}
	envelope := wire.GetSigned()
	if envelope == nil {
		t.Fatalf("substituted request = %#v", wire.GetVariant())
	}
	if !bytes.Equal(envelope.GetPayload(), payload) {
		t.Fatal("the envelope payload is not the exact batch the plugin serialized")
	}
	if err := signing.Verify(envelope, public); err != nil {
		t.Fatalf("the server rejected the signature: %v", err)
	}
	batch, err := signing.ExtractBatch(envelope)
	if err != nil {
		t.Fatalf("the server could not extract the batch: %v", err)
	}
	if batch.GetIdempotencyKey() != "batch-1" || len(batch.GetRequests()) != 1 {
		t.Fatalf("extracted batch = %#v", batch)
	}
	metadata := batch.GetRequests()[0].GetApply().GetAction().GetAddMetadata().GetMetadata()
	if len(metadata) != 5 || metadata["tier"].GetStringValue() != "gold" {
		t.Fatalf("extracted metadata = %#v", metadata)
	}
}

// liftUnsignedApplyBatch takes the exact wire bytes of the unsigned variant,
// the way the host does. It never re-serializes: a second serialization could
// order the metadata map differently, and the signature would then cover bytes
// the server never receives.
func liftUnsignedApplyBatch(t *testing.T, message []byte) ([]byte, int) {
	t.Helper()
	var payload []byte
	found := 0
	for remainder := message; len(remainder) > 0; {
		number, kind, consumed := protowire.ConsumeTag(remainder)
		if consumed < 0 {
			t.Fatalf("malformed apply request: %v", protowire.ParseError(consumed))
		}
		length := protowire.ConsumeFieldValue(number, kind, remainder[consumed:])
		if length < 0 {
			t.Fatalf("malformed field %d", number)
		}
		if number == applyRequestUnsignedField {
			value, valueLength := protowire.ConsumeBytes(remainder[consumed:])
			if valueLength < 0 {
				t.Fatal("malformed unsigned variant")
			}
			payload = value
			found++
		}
		remainder = remainder[consumed+length:]
	}
	return payload, found
}

func signedVariants(t *testing.T, message []byte) int {
	t.Helper()
	found := 0
	for remainder := message; len(remainder) > 0; {
		number, kind, consumed := protowire.ConsumeTag(remainder)
		if consumed < 0 {
			t.Fatal("malformed apply request")
		}
		length := protowire.ConsumeFieldValue(number, kind, remainder[consumed:])
		if length < 0 {
			t.Fatal("malformed field")
		}
		if number == applyRequestSignedField {
			found++
		}
		remainder = remainder[consumed+length:]
	}
	return found
}

// substituteSignedApplyBatch mirrors the host's rewrite: the unsigned variant
// is replaced by the envelope and every other field is carried through byte
// for byte, because skip_response and forwarded_caller_snapshot sit outside
// the signed unit by design.
func substituteSignedApplyBatch(t *testing.T, message []byte, envelope *signaturepb.SignedApplyBatch) []byte {
	t.Helper()
	encoded := protowire.AppendTag(nil, signedApplyBatchKeyIDField, protowire.BytesType)
	encoded = protowire.AppendString(encoded, envelope.GetKeyId())
	encoded = protowire.AppendTag(encoded, signedApplyBatchSignatureField, protowire.BytesType)
	encoded = protowire.AppendBytes(encoded, envelope.GetSignature())
	encoded = protowire.AppendTag(encoded, signedApplyBatchPayloadField, protowire.BytesType)
	encoded = protowire.AppendBytes(encoded, envelope.GetPayload())

	variant := protowire.AppendTag(nil, applyRequestSignedField, protowire.BytesType)
	variant = protowire.AppendBytes(variant, encoded)

	substituted := make([]byte, 0, len(message)+len(variant))
	for remainder := message; len(remainder) > 0; {
		number, kind, consumed := protowire.ConsumeTag(remainder)
		if consumed < 0 {
			t.Fatal("malformed apply request")
		}
		length := protowire.ConsumeFieldValue(number, kind, remainder[consumed:])
		if length < 0 {
			t.Fatal("malformed field")
		}
		if number == applyRequestUnsignedField {
			substituted = append(substituted, variant...)
		} else {
			substituted = append(substituted, remainder[:consumed+length]...)
		}
		remainder = remainder[consumed+length:]
	}
	return substituted
}
