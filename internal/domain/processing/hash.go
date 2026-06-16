// Package processing computes the chained audit hash that anchors every
// FSM proposal to the cluster's append-only audit log.
//
// The hash is keyed by a value derived from the immutable ClusterID, so
// an attacker who learns the hashing algorithm but not the ClusterID
// cannot forge audit entries offline. ClusterID immutability is
// enforced by bootstrap.ValidateOrPersistConfig; the per-algorithm key
// derivation uses domain-separated BLAKE3.
//
// The generator hashes opaque byte slices, never a proto. The apply
// path assembles those slices from canonical binary encodings of every
// bound AuditEntry and AuditItem field (cf. state.BuildHashedHeaderPayload
// and state.BuildPerItemPayload). The verifier reassembles the same
// slices from the persisted entry and items, so reproducing the chain
// requires only the stored bytes + the ClusterID — no proto schema, no
// vtprotobuf, no marshaller version.
package processing

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// HashGenerator computes the chained audit hash for FSM proposals using
// a per-cluster keyed algorithm. One implementation per supported
// HashAlgorithm (xxh3HashGenerator, blake3HashGenerator).
//
// Generators are immutable once constructed: the per-cluster key is
// derived from the ClusterID at construction time and never changes.
// When the cluster config swaps the active algorithm, a new generator
// is constructed via NewHashGenerator and replaces the previous one.
type HashGenerator interface {
	// Compute hashes (concat(slices) || lastHash) and returns the
	// reusable buffer plus the resulting hash. Buf is truncated and
	// re-grown so callers can amortize allocations across proposals.
	// `slices` is the ordered list of canonical byte payloads that bind
	// the AuditEntry header and each AuditItem (the apply path builds
	// them via state.BuildHashedHeaderPayload and BuildPerItemPayload).
	// The generator never marshals anything itself.
	Compute(buf []byte, lastHash []byte, slices [][]byte) (resBuf []byte, hash []byte)
	// Algorithm returns the enum stamped on each AuditEntry's
	// HashVersion field, so the checker can replay using the same impl.
	Algorithm() commonpb.HashAlgorithm
}

// NewHashGenerator selects the implementation matching algorithm and
// derives its per-cluster key from clusterID. Use this both in fx
// wiring (FSM) and per-entry in the checker (algorithm read from
// AuditEntry.HashVersion).
//
// Any algorithm value other than HASH_ALGORITHM_XXH3 falls through to
// BLAKE3 (the default, value 0). This preserves the lenient behavior of
// the previous free-function code path and keeps the checker robust
// against future enum values or stale data.
func NewHashGenerator(algorithm commonpb.HashAlgorithm, clusterID string) HashGenerator {
	switch algorithm {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return newXXH3HashGenerator(clusterID)
	default:
		return newBLAKE3HashGenerator(clusterID)
	}
}

// serializeAuditPayload writes (concat(slices) || lastHash) into buf and
// returns the populated slice. Shared by all HashGenerator implementations
// so the on-wire shape of what gets hashed stays algorithm-independent.
func serializeAuditPayload(buf []byte, lastHash []byte, slices [][]byte) []byte {
	buf = buf[:0]
	for _, s := range slices {
		buf = append(buf, s...)
	}

	if len(lastHash) > 0 {
		buf = append(buf, lastHash...)
	}

	return buf
}
