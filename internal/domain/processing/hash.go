// Package processing computes the chained audit hash that anchors every
// FSM proposal to the cluster's append-only audit log.
//
// The hash is keyed by a value derived from the immutable ClusterID, so
// an attacker who learns the hashing algorithm but not the ClusterID
// cannot forge audit entries offline. ClusterID immutability is
// enforced by bootstrap.ValidateOrPersistConfig; the per-algorithm key
// derivation uses domain-separated BLAKE3.
package processing

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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
	// Compute hashes (serialized orders || lastHash) and returns the
	// reusable buffer plus the resulting hash. Buf is truncated and
	// re-grown so callers can amortize allocations across proposals.
	Compute(buf []byte, lastHash []byte, orders []*raftcmdpb.Order) (resBuf []byte, hash []byte)
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

// serializeAuditPayload writes (orders || lastHash) into buf and returns
// the populated slice. Shared by all HashGenerator implementations so
// the wire format of what gets hashed stays algorithm-independent.
func serializeAuditPayload(buf []byte, lastHash []byte, orders []*raftcmdpb.Order) []byte {
	buf = buf[:0]
	for _, order := range orders {
		buf = order.MarshalDeterministicVT(buf)
	}

	if len(lastHash) > 0 {
		buf = append(buf, lastHash...)
	}

	return buf
}
