package state

import (
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// failingReader wraps a real RecoveryReader and always returns an injected
// error from Get. NewReadHandle / NewDirectReadHandle proxy through so that
// recovery can still open a handle — the Get failure is the one we want to
// trigger inside LoadFSMStateFromStore.
type failingReader struct {
	inner dal.RecoveryReader
	err   error
}

func (r failingReader) Get([]byte) ([]byte, io.Closer, error) {
	return nil, nil, r.err
}

func (r failingReader) NewDirectReadHandle() (*dal.ReadHandle, error) {
	return r.inner.NewDirectReadHandle()
}

func (r failingReader) NewReadHandle() (*dal.ReadHandle, error) {
	return r.inner.NewReadHandle()
}

// TestRecoverStateAtomicOnLoadFailure asserts the headline build-and-swap
// property: if LoadFSMStateFromStore returns an error, Machine.State is left
// completely untouched (same pointer, same field values). The store-derived
// fields the caller would otherwise have inspected to assess "did anything
// change?" stay at their pre-call values.
func TestRecoverStateAtomicOnLoadFailure(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)

	// Capture pre-recovery state. newTestMachine already triggered a
	// successful RecoverState so machine.State is the "current" state.
	stateBefore := machine.State
	snapshot := *machine.State

	injected := errors.New("simulated pebble Get failure")
	recovery := NewRecovery(machine, failingReader{inner: store, err: injected})

	err := recovery.RecoverState()
	require.Error(t, err)
	require.ErrorIs(t, err, injected,
		"the injected reader error must propagate (load happens before any swap)")

	// Same pointer => RestoreState was never called.
	require.Same(t, stateBefore, machine.State,
		"failed RecoverState must not swap the state pointer")

	// Field values unchanged (defence in depth in case future code mutates
	// the existing struct before the swap).
	require.Equal(t, snapshot.LastAppliedIndex, machine.State.LastAppliedIndex)
	require.Equal(t, snapshot.LastAppliedTimestamp, machine.State.LastAppliedTimestamp)
	require.Equal(t, snapshot.NextSequenceID, machine.State.NextSequenceID)
	require.Equal(t, snapshot.NextAuditSequenceID, machine.State.NextAuditSequenceID)
	require.Equal(t, snapshot.NextLedgerID, machine.State.NextLedgerID)
	require.Equal(t, snapshot.NextQueryCheckpointID, machine.State.NextQueryCheckpointID)
	require.Equal(t, snapshot.CacheEpoch, machine.State.CacheEpoch)
}

func TestRecoverStateRejectsPersistedExhaustedSequencesAtomically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		counter domain.SequenceCounter
		seed    func(*testing.T, *dal.Store)
	}{
		{
			name:    "global log",
			counter: domain.SequenceCounterLog,
			seed: func(t *testing.T, store *dal.Store) {
				appendLogs(t, store, 1, &commonpb.Log{Sequence: math.MaxUint64})
			},
		},
		{
			name:    "audit",
			counter: domain.SequenceCounterAudit,
			seed: func(t *testing.T, store *dal.Store) {
				batch := store.OpenWriteSession()
				require.NoError(t, appendAuditEntries(batch, &auditpb.AuditEntry{Sequence: math.MaxUint64}))
				require.NoError(t, batch.Commit())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			machine, store, _ := newTestMachine(t)
			stateBefore := machine.State
			snapshot := *stateBefore
			tt.seed(t, store)

			err := NewRecovery(machine, store).RecoverState()
			var exhausted *domain.ErrSequenceExhausted
			require.ErrorAs(t, err, &exhausted)
			require.Equal(t, tt.counter, exhausted.Counter)
			require.Same(t, stateBefore, machine.State)
			require.Equal(t, snapshot.NextSequenceID, machine.State.NextSequenceID)
			require.Equal(t, snapshot.NextAuditSequenceID, machine.State.NextAuditSequenceID)
		})
	}
}
