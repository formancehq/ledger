package auditpb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestAuditJSONPreservesExactHTTPRequestProof(t *testing.T) {
	t.Parallel()
	proof := &signaturepb.SignedHTTPRequest{
		Method: "POST", Scheme: "https", RequestTarget: "/v3/main/transactions?atomic=true", Authority: "ledger.example",
		ContentType: "application/json", ContentDigest: "sha-256=:digest:", IdempotencyKey: "batch-1",
		SignatureInput: `sig1=("@method");keyid="ops";alg="ed25519"`, Signature: "sig1=:signature:", Body: []byte("{\n \"reference\":\"r\"\n}\n"),
	}
	encoded, err := json.Marshal(&auditpb.AuditEntry{Sequence: 1, HttpSignature: proof})
	require.NoError(t, err)
	var visible map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &visible))
	require.Contains(t, visible, "httpSignature")
	require.NotContains(t, visible, "http_signature")
	recovered := &signaturepb.SignedHTTPRequest{}
	require.NoError(t, protojson.Unmarshal(visible["httpSignature"], recovered))
	require.True(t, proto.Equal(proof, recovered), "REST audit reads must expose the exact proof required to verify the chain")

	encoded, err = json.Marshal(&auditpb.AuditEntry{Sequence: 1})
	require.NoError(t, err)
	visible = nil
	require.NoError(t, json.Unmarshal(encoded, &visible))
	require.NotContains(t, visible, "httpSignature", "historical entries acquire no synthetic proof")

	proof.Authority = string([]byte{0xff})
	_, err = json.Marshal(&auditpb.AuditEntry{Sequence: 1, HttpSignature: proof})
	require.ErrorContains(t, err, "marshaling httpSignature", "serialization errors must not silently omit chain-bound evidence")
}
