package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestHTTPProofAuditHeaderPreservesHistoricalGolden(t *testing.T) {
	t.Parallel()
	entry := &auditpb.AuditEntry{Sequence: 1, OrderCount: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 1, MaxLogSequence: 1}}}
	before, err := BuildHashedHeaderPayload(entry)
	require.NoError(t, err)
	require.Equal(t, goldenBuildHeader(entry), before, "released hash preimages have no HTTP placeholder")
	entry.HttpSignature = &signaturepb.SignedHTTPRequest{
		Method: "POST", RequestTarget: "/v3/main/transactions", Authority: "ledger.example",
		ContentType: "application/json", ContentDigest: "digest", IdempotencyKey: "idem",
		SignatureInput: "input", Signature: "signature", Body: []byte("body"),
	}
	// Independent encoding oracle: fixed field order and uint32 big-endian
	// lengths, followed by body and raw unknown bytes. The released golden
	// builder stays unchanged to pin all historical fields separately.
	var proof []byte
	for _, value := range []string{"POST", "/v3/main/transactions", "ledger.example", "application/json", "digest", "idem", "input", "signature", "body", "", ""} {
		proof = goldenLenString(proof, value)
	}
	want := goldenLenString(append([]byte(nil), before...), "ledger:http-signature:v1")
	want = goldenLenBytes(want, proof)
	got, err := BuildHashedHeaderPayload(entry)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestHTTPProofAuditHeaderBindsEveryField(t *testing.T) {
	t.Parallel()
	entry := &auditpb.AuditEntry{OrderCount: 1, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}}, HttpSignature: &signaturepb.SignedHTTPRequest{Method: "POST"}}
	before, err := BuildHashedHeaderPayload(entry)
	require.NoError(t, err)
	fields := entry.GetHttpSignature().ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		t.Run(string(field.Name()), func(t *testing.T) {
			t.Parallel()
			changed := entry.CloneVT()
			message := changed.GetHttpSignature().ProtoReflect()
			value := protoreflect.ValueOfString("changed")
			if field.Name() == "body" {
				value = protoreflect.ValueOfBytes([]byte("changed"))
			}
			message.Set(field, value)
			after, err := BuildHashedHeaderPayload(changed)
			require.NoError(t, err)
			require.NotEqual(t, before, after)
		})
	}
	changed := entry.CloneVT()
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("future evidence"))
	changed.GetHttpSignature().ProtoReflect().SetUnknown(unknown)
	after, err := BuildHashedHeaderPayload(changed)
	require.NoError(t, err)
	require.NotEqual(t, before, after, "unknown proof fields are also audit-bound")
}
