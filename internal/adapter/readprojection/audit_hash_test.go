package readprojection

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// Exercise the actual audit preimage builders and hash primitive used by the
// FSM and checker: a public projection must never rewrite their evidence.
func TestAuditProjectionPreservesAuthoritativeHash(t *testing.T) {
	t.Parallel()

	for _, signed := range []bool{false, true} {
		name := "unsigned"
		if signed {
			name = "signed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := &commonpb.SinkConfig{
				Name: "fixture",
				Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
					Endpoint: "https://example.test/hook", Secret: "hash-bound-credential",
				}},
			}
			entry := &auditpb.AuditEntry{
				Sequence: 7, OrderCount: 1,
				Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
					MinLogSequence: 12, MaxLogSequence: 12,
				}},
				Items: []*auditpb.AuditItem{{
					LogSequence:     12,
					SerializedOrder: processing.MarshalOrderBusinessIntent(addSinkOrder(config), nil),
				}},
			}
			if signed {
				payload, err := (&servicepb.ApplyBatch{Requests: []*servicepb.Request{{
					Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config}},
				}}}).MarshalVT()
				require.NoError(t, err)
				key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
				entry.Signature = &signaturepb.SignedApplyBatch{
					KeyId: "fixture", Payload: payload, Signature: ed25519.Sign(key, payload),
				}
			}
			generator := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "fixture-cluster")
			previousHash := []byte("previous-authoritative-hash")
			compute := func(value *auditpb.AuditEntry) []byte {
				header, err := state.BuildHashedHeaderPayload(value)
				require.NoError(t, err)
				payloads := [][]byte{header}
				for _, item := range value.GetItems() {
					payloads = append(payloads, state.BuildPerItemPayload(item))
				}
				_, hash := generator.Compute(nil, previousHash, payloads)

				return hash
			}
			entry.HashVersion = uint32(generator.Algorithm())
			entry.Hash = compute(entry)
			original := proto.Clone(entry)

			projected, err := Audit(entry)
			require.NoError(t, err)
			require.True(t, proto.Equal(original, entry), "projection changed authoritative evidence")
			require.Equal(t, entry.GetHash(), compute(entry), "authoritative chain must still verify")
			require.Equal(t, entry.GetHash(), projected.GetHash(), "display retains original evidence identity")
			require.NotContains(t, string(projected.GetItems()[0].GetSerializedOrder()), "hash-bound-credential")
			require.NotEqual(t, projected.GetHash(), compute(projected), "display bytes are not the original hash preimage")
		})
	}
}
