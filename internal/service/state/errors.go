package state

import "fmt"

// ErrNodeOutOfSync is returned when the snapshot index exceeds the last applied index,
// indicating the node has fallen behind and cannot apply entries.
type ErrNodeOutOfSync struct {
	SnapshotIndex    uint64
	LastAppliedIndex uint64
}

func (e *ErrNodeOutOfSync) Error() string {
	return fmt.Sprintf(
		"last snapshot index is %d, expecting lower than %d, node out of sync",
		e.SnapshotIndex, e.LastAppliedIndex,
	)
}

// ErrInvalidEntryIndex is returned when a raft entry has an unexpected index,
// indicating a gap or duplication in the log sequence.
type ErrInvalidEntryIndex struct {
	ReceivedIndex uint64
	ExpectedIndex uint64
}

func (e *ErrInvalidEntryIndex) Error() string {
	return fmt.Sprintf("invalid index, got %d, expected %d", e.ReceivedIndex, e.ExpectedIndex)
}

// ErrGenerationMismatch is returned when preloaded data targets a cache generation
// that does not match either gen0 or gen1.
type ErrGenerationMismatch struct {
	LastPersistedIndex uint64
	Gen0BaseIndex      uint64
	Gen1BaseIndex      uint64
}

func (e *ErrGenerationMismatch) Error() string {
	return fmt.Sprintf(
		"invalid preload, generation mismatch: lastPersistedIndex=%d, gen0=%d, gen1=%d",
		e.LastPersistedIndex, e.Gen0BaseIndex, e.Gen1BaseIndex,
	)
}

// ErrDoubleEntryInvariantViolated is returned when the sum of input deltas
// does not equal the sum of output deltas, indicating a broken accounting invariant.
type ErrDoubleEntryInvariantViolated struct {
	InputSum  string
	OutputSum string
}

func (e *ErrDoubleEntryInvariantViolated) Error() string {
	return fmt.Sprintf(
		"double-entry invariant violated: sum of inputs (%s) != sum of outputs (%s)",
		e.InputSum, e.OutputSum,
	)
}
