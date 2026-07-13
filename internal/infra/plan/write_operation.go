package plan

// WriteOperation describes one of the writes a proposal carries (an
// Order or a TechnicalUpdate). Run consumes a slice of these to
// (1) aggregate per-operation Coverage for Build, and (2) compute each
// operation's coverage_bits and assign them onto the proto right
// before each marshal — both on the happy path and on the rare
// rebuild under the proposal guard.
//
// The two fields are independent: an operation may declare reads
// without wiring Target (test scenarios), and an operation with no
// reads may still want a (zero) bitset assigned to pin "I read
// nothing" explicitly. In practice the common cases are either both
// filled (admission orders, mirror cursor) or both nil (cluster
// config, idempotency eviction, events sink — TUs whose handlers
// don't read cache state).
type WriteOperation struct {
	// Coverage declares which cache keys this operation will read at FSM
	// apply time. nil or empty Coverage means "no reads".
	Coverage *Coverage

	// Target is the address of the []byte field the computed bitset
	// must be written into (Order.CoverageBits, TechnicalUpdate.CoverageBits,
	// etc.). Passing a pointer instead of a closure eliminates the
	// per-operation heap allocation that a captured index would force
	// — admission builds N of these on every proposal.
	//
	// A nil Target means "discard the bitset" (test-only pattern).
	Target *[]byte
}
