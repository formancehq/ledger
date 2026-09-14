package ledgerv3

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"
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
	applyRequestCallerField       = protowire.Number(3)
	applyRequestSkipResponseField = protowire.Number(4)

	signedApplyBatchKeyIDField     = protowire.Number(1)
	signedApplyBatchSignatureField = protowire.Number(2)
	signedApplyBatchPayloadField   = protowire.Number(3)
)

const (
	ordinaryApplyPayloadBytes  uint64 = 262140
	ordinarySignedMessageBytes uint64 = 263241
	artifactApplyPayloadBytes  uint64 = 2097148
	artifactSignedMessageBytes uint64 = 2098250
	maxSigningKeyIDBytes       uint32 = 1024
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

// Removing the operation identity from the signing declaration lets a signer
// bind the historical synthetic ledger.batches.apply token rather than the
// operation the plugin actually sends. This table independently pins every
// command-to-operation mapping and the opaque rewrite envelope admitted for it.
func TestEverySignedCommandDeclaresItsExactOpaqueApplyOperation(t *testing.T) {
	t.Parallel()

	wantOperations := map[string]string{
		"ledger.v3.account-types.add":                     "ledger.v3.Apply.AddAccountType",
		"ledger.v3.account-types.remove":                  "ledger.v3.Apply.RemoveAccountType",
		"ledger.v3.account-types.set-default-enforcement": "ledger.v3.Apply.SetDefaultEnforcementMode",
		"ledger.v3.accounts.delete-metadata":              "ledger.v3.Apply.DeleteMetadata",
		"ledger.v3.accounts.set-metadata":                 "ledger.v3.Apply.AddMetadata",
		"ledger.v3.indexes.create":                        "ledger.v3.Apply.CreateIndex",
		"ledger.v3.indexes.drop":                          "ledger.v3.Apply.DropIndex",
		"ledger.v3.ledgers.configuration.apply":           "ledger.v3.Apply.Configuration",
		"ledger.v3.ledgers.create":                        "ledger.v3.Apply.CreateLedger",
		"ledger.v3.ledgers.delete":                        "ledger.v3.Apply.DeleteLedger",
		"ledger.v3.ledgers.delete-metadata":               "ledger.v3.Apply.DeleteLedgerMetadata",
		"ledger.v3.ledgers.remove-metadata-type":          "ledger.v3.Apply.RemoveMetadataFieldType",
		"ledger.v3.ledgers.set-metadata":                  "ledger.v3.Apply.SaveLedgerMetadata",
		"ledger.v3.ledgers.set-metadata-type":             "ledger.v3.Apply.SetMetadataFieldType",
		"ledger.v3.numscripts.save":                       "ledger.v3.Apply.SaveNumscript",
		"ledger.v3.queries.create":                        "ledger.v3.Apply.CreatePreparedQuery",
		"ledger.v3.queries.delete":                        "ledger.v3.Apply.DeletePreparedQuery",
		"ledger.v3.queries.update":                        "ledger.v3.Apply.UpdatePreparedQuery",
		"ledger.v3.transactions.create":                   "ledger.v3.Apply.CreateTransaction",
		"ledger.v3.transactions.delete-metadata":          "ledger.v3.Apply.DeleteMetadata",
		"ledger.v3.transactions.revert":                   "ledger.v3.Apply.RevertTransaction",
		"ledger.v3.transactions.set-metadata":             "ledger.v3.Apply.AddMetadata",
	}
	largeOperations := map[string]bool{
		"ledger.v3.Apply.Configuration":     true,
		"ledger.v3.Apply.CreateLedger":      true,
		"ledger.v3.Apply.CreateTransaction": true,
		"ledger.v3.Apply.SaveNumscript":     true,
	}
	wantPassthrough := []sdk.ProtobufField{
		{Number: uint32(applyRequestCallerField), WireType: uint32(protowire.BytesType)},
		{Number: uint32(applyRequestSkipResponseField), WireType: uint32(protowire.VarintType)},
	}

	seen := 0
	seenOperations := make(map[string]struct{})
	for _, command := range (Plugin{}).Commands() {
		wantOperation, signed := wantOperations[command.ID]
		if !signed {
			if command.RequestSigning != nil {
				t.Fatalf("unsigned command %q declares signing", command.ID)
			}
			continue
		}
		seen++
		seenOperations[wantOperation] = struct{}{}
		signing := command.RequestSigning
		if signing == nil {
			t.Fatalf("signed command %q has no signing declaration", command.ID)
		}
		if signing.OperationID != wantOperation || signing.Capability != sdk.CapabilitySignLedgerApplyBatch ||
			signing.ProductMajor != productMajor || signing.PayloadType != signingPayloadType || signing.Algorithm != "Ed25519" {
			t.Fatalf("command %q signing = %#v, want operation %q", command.ID, signing, wantOperation)
		}

		payloadBytes, signedBytes := ordinaryApplyPayloadBytes, ordinarySignedMessageBytes
		if largeOperations[wantOperation] {
			payloadBytes, signedBytes = artifactApplyPayloadBytes, artifactSignedMessageBytes
		}
		if signing.Protobuf == nil {
			t.Fatalf("command %q has no opaque protobuf recipe", command.ID)
		}
		recipe := signing.Protobuf
		if recipe.UnsignedField != uint32(applyRequestUnsignedField) || recipe.SignedField != uint32(applyRequestSignedField) ||
			recipe.EnvelopeKeyIDField != uint32(signedApplyBatchKeyIDField) ||
			recipe.EnvelopeSignatureField != uint32(signedApplyBatchSignatureField) ||
			recipe.EnvelopePayloadField != uint32(signedApplyBatchPayloadField) ||
			!reflect.DeepEqual(recipe.PassthroughFields, wantPassthrough) ||
			recipe.MaxPayloadBytes != payloadBytes || recipe.MaxSignedMessageBytes != signedBytes ||
			recipe.MaxKeyIDBytes != maxSigningKeyIDBytes || recipe.SignatureLength != ed25519.SignatureSize {
			t.Fatalf("command %q opaque recipe = %#v", command.ID, recipe)
		}

		operation, ok := operationByID(command, wantOperation)
		if !ok || operation.Service != sdk.ServiceLedger || operation.GRPC == nil ||
			operation.GRPC.FullMethod != signedApplyFullMethod || operation.GRPC.ServerStreaming ||
			operation.GRPC.GeneratedClient == nil || uint64(operation.GRPC.GeneratedClient.MaxRequestMessageBytes) != signedBytes {
			t.Fatalf("command %q signed operation = %#v", command.ID, operation)
		}
	}
	if seen != len(wantOperations) {
		t.Fatalf("signed commands = %d, want %d", seen, len(wantOperations))
	}
	if len(seenOperations) != 20 {
		t.Fatalf("distinct signed operations = %d, want 20", len(seenOperations))
	}
}

// The two declared signed-message ceilings are not estimates. This test uses
// Ledger's generated protobuf messages to prove the exact boundary with a
// maximum-sized key ID and Ed25519 signature, including the extra outer
// length-prefix byte in the 2 MiB profile.
func TestOpaqueSigningCeilingsFitTheRealLedgerWireMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		payloadBytes  uint64
		unsignedBytes int
		signedBytes   uint64
	}{
		{name: "ordinary", payloadBytes: ordinaryApplyPayloadBytes, unsignedBytes: int(applyRequestBytes), signedBytes: ordinarySignedMessageBytes},
		{name: "artifact", payloadBytes: artifactApplyPayloadBytes, unsignedBytes: int(artifactApplyRequestBytes), signedBytes: artifactSignedMessageBytes},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// ApplyBatch.idempotency_key is field 2. Choosing this value length
			// produces the exact desired serialized batch size without unknown
			// fields or a test reimplementation of protobuf encoding.
			batch := &servicepb.ApplyBatch{IdempotencyKey: string(make([]byte, test.payloadBytes-4))}
			payload, err := proto.Marshal(batch)
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(payload)) != test.payloadBytes {
				t.Fatalf("payload bytes = %d, want %d", len(payload), test.payloadBytes)
			}
			unsigned, err := proto.Marshal(&servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: batch}})
			if err != nil {
				t.Fatal(err)
			}
			if len(unsigned) != test.unsignedBytes {
				t.Fatalf("unsigned message bytes = %d, want %d", len(unsigned), test.unsignedBytes)
			}
			signed, err := proto.Marshal(&servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Signed{Signed: &signaturepb.SignedApplyBatch{
				KeyId: string(make([]byte, maxSigningKeyIDBytes)), Signature: make([]byte, ed25519.SignatureSize), Payload: payload,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			if uint64(len(signed)) != test.signedBytes {
				t.Fatalf("signed message bytes = %d, want %d", len(signed), test.signedBytes)
			}
		})
	}
}

// The field numbers the host substitution depends on are the ones the pinned
// Ledger descriptors actually declare.
func TestSignedApplyWireContractMatchesThePinnedDescriptors(t *testing.T) {
	t.Parallel()

	request := servicepb.File_bucket_proto.Messages().ByName("ApplyRequest")
	if request == nil || request.FullName() != "ledger.ApplyRequest" {
		t.Fatal("pinned ApplyRequest descriptor is unavailable")
	}
	for name, want := range map[protoreflect.Name]struct {
		number protowire.Number
		kind   protoreflect.Kind
	}{
		"unsigned":                  {applyRequestUnsignedField, protoreflect.MessageKind},
		"signed":                    {applyRequestSignedField, protoreflect.MessageKind},
		"forwarded_caller_snapshot": {applyRequestCallerField, protoreflect.MessageKind},
		"skip_response":             {applyRequestSkipResponseField, protoreflect.BoolKind},
	} {
		field := request.Fields().ByName(name)
		if field == nil || protowire.Number(field.Number()) != want.number || field.Kind() != want.kind {
			t.Fatalf("ApplyRequest.%s = %v, want field %d kind %s", name, field, want.number, want.kind)
		}
	}
	unsigned := request.Fields().ByName("unsigned")
	signed := request.Fields().ByName("signed")
	if unsigned.ContainingOneof() == nil || unsigned.ContainingOneof() != signed.ContainingOneof() {
		t.Fatal("ApplyRequest unsigned and signed are not members of the same oneof")
	}
	if unsigned.Message().FullName() != signedApplyBatchFullName || signed.Message().FullName() != "signature.SignedApplyBatch" {
		t.Fatalf("ApplyRequest variant messages = %s / %s", unsigned.Message().FullName(), signed.Message().FullName())
	}
	envelope := signaturepb.File_signature_proto.Messages().ByName("SignedApplyBatch")
	if envelope == nil || envelope.FullName() != "signature.SignedApplyBatch" {
		t.Fatal("pinned SignedApplyBatch descriptor is unavailable")
	}
	for name, want := range map[protoreflect.Name]struct {
		number protowire.Number
		kind   protoreflect.Kind
	}{
		"key_id":    {signedApplyBatchKeyIDField, protoreflect.StringKind},
		"signature": {signedApplyBatchSignatureField, protoreflect.BytesKind},
		"payload":   {signedApplyBatchPayloadField, protoreflect.BytesKind},
	} {
		field := envelope.Fields().ByName(name)
		if field == nil || protowire.Number(field.Number()) != want.number || field.Kind() != want.kind {
			t.Fatalf("SignedApplyBatch.%s = %v, want field %d kind %s", name, field, want.number, want.kind)
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
