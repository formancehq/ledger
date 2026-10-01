package http

import (
	"net/http"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// handleListSigningKeys handles GET /signing-keys to list registered
// Ed25519 signing keys, paged by key id.
//
// This route performs a live read (linearizable unless the caller sends
// `X-Consistency: stale`, see readConsistency); signing-key reads are
// live-only on both transports.
func (s *Server) handleListSigningKeys(w http.ResponseWriter, r *http.Request) {
	page, ok := parsePageQuery(w, r)
	if !ok {
		return
	}

	cursor, err := s.backend.ListSigningKeys(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	keys, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	keyID := func(k *commonpb.SigningKey) string { return k.GetKeyId() }
	slices.SortFunc(keys, func(a, b *commonpb.SigningKey) int { return strings.Compare(keyID(a), keyID(b)) })

	keys, links := pageSorted(page, keys, keyID)

	data, err := protoListJSON(keys)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", err)

		return
	}

	writePageOK(w, r, data, links)
}
