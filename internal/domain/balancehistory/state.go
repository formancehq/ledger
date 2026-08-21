package balancehistory

import (
	"fmt"
)

// IncarnationState is one deterministic ledger-name/ID mapping in a reducer
// snapshot. Active and Seen are sorted by ID before persistence.
type IncarnationState struct {
	Name string `json:"name"`
	ID   uint32 `json:"id"`
}

// State is the minimal durable reducer state needed to resume tailing without
// replaying every lifecycle log on each restart.
type State struct {
	Active  []IncarnationState `json:"active,omitempty"`
	Seen    []IncarnationState `json:"seen,omitempty"`
	Enabled []string           `json:"enabled,omitempty"`
	Last    Position           `json:"last"`
	HasLast bool               `json:"hasLast"`
}

// NewReducerFromState validates and restores a durable reducer snapshot.
func NewReducerFromState(state State) (*Reducer, error) {
	reducer := NewReducer()
	for _, ledger := range state.Enabled {
		if ledger == "" || reducer.enabled[ledger] {
			return nil, fmt.Errorf("%w: invalid enabled ledger %q", ErrInvalidLifecycle, ledger)
		}
		reducer.enabled[ledger] = true
	}
	seenNames := make(map[string]uint32, len(state.Seen))
	for _, incarnation := range state.Seen {
		if incarnation.ID == 0 || incarnation.Name == "" {
			return nil, fmt.Errorf("%w: invalid seen incarnation snapshot", ErrInvalidLifecycle)
		}
		if previous, exists := reducer.seenIDs[incarnation.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate seen incarnation id %d (%q and %q)", ErrInvalidLifecycle, incarnation.ID, previous, incarnation.Name)
		}
		if previous, exists := seenNames[incarnation.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate seen ledger name %q (ids %d and %d)", ErrInvalidLifecycle, incarnation.Name, previous, incarnation.ID)
		}
		reducer.seenIDs[incarnation.ID] = incarnation.Name
		seenNames[incarnation.Name] = incarnation.ID
	}
	for _, incarnation := range state.Active {
		if incarnation.ID == 0 || incarnation.Name == "" {
			return nil, fmt.Errorf("%w: invalid active incarnation snapshot", ErrInvalidLifecycle)
		}
		if previous, exists := reducer.activeByName[incarnation.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate active ledger %q (%d and %d)", ErrInvalidLifecycle, incarnation.Name, previous, incarnation.ID)
		}
		seenName, seen := reducer.seenIDs[incarnation.ID]
		if !seen || seenName != incarnation.Name {
			return nil, fmt.Errorf("%w: active incarnation %q/%d is absent from seen set", ErrInvalidLifecycle, incarnation.Name, incarnation.ID)
		}
		reducer.activeByName[incarnation.Name] = incarnation.ID
	}
	for ledger := range reducer.enabled {
		if _, active := reducer.activeByName[ledger]; !active {
			return nil, fmt.Errorf("%w: enabled ledger %q is not active", ErrInvalidLifecycle, ledger)
		}
	}
	if state.HasLast && (state.Last.AuditSequence == 0 || state.Last.LogSequence == 0) {
		return nil, fmt.Errorf("%w: invalid last reducer position", ErrInvalidPosition)
	}
	reducer.last = state.Last
	reducer.hasLast = state.HasLast

	return reducer, nil
}
