package http

import (
	"encoding/hex"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// signingKeyDTO is the HTTP response shape for a registered signing key.
//
// Colocated here because this handler is its only consumer. The public key is
// hex-encoded, matching every other byte field on this API and the
// "PUBLIC KEY (HEX)" column of `ledgerctl signing list-keys`; protojson emitted
// base64. commonpb.SigningKey deliberately carries no MarshalJSON — adding one
// would also reshape CLI output, and misc/operator parses that in a separate Go
// module a root `go build ./...` never compiles. See dto_indexes.go.
type signingKeyDTO struct {
	KeyID     string `json:"keyId"`
	PublicKey string `json:"publicKey"`
	// No omitempty: "" means this key is a root key, which differs from absent.
	ParentKeyID string `json:"parentKeyId"`
}

func newSigningKeyDTOList(src []*commonpb.SigningKey) []signingKeyDTO {
	out := make([]signingKeyDTO, 0, len(src))

	for _, k := range src {
		// A nil element would be a backend bug. Skip rather than deref: this is a
		// read-only ops surface, so degrading the list beats a 500, and nothing
		// can desync from it.
		if k == nil {
			continue
		}

		out = append(out, signingKeyDTO{
			KeyID:       k.GetKeyId(),
			PublicKey:   hex.EncodeToString(k.GetPublicKey()),
			ParentKeyID: k.GetParentKeyId(),
		})
	}

	return out
}

// handleListSigningKeys handles GET /signing-keys to list registered
// Ed25519 signing keys.
//
// Live, best-effort read: it drains the full cursor and does not expose the
// gRPC read-consistency options (checkpointId / minLogSequence) or a
// bidirectional cursor. Clients needing consistency-bounded reads use gRPC.
// Tracked follow-up (same carve-out as ListTransactions / the audit reads).
func (s *Server) handleListSigningKeys(w http.ResponseWriter, r *http.Request) {
	cursor, err := s.backend.ListSigningKeys(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	keys, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	writeOKChecked(w, r, newSigningKeyDTOList(keys))
}
