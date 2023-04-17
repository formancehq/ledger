package command

import (
	"github.com/formancehq/ledger/pkg/core"
)

type Reserve struct {
	state    *State
	inFlight *inFlight
}

func (r *Reserve) Clear(transaction *core.Transaction) {

	r.state.mu.Lock()
	defer r.state.mu.Unlock()

	if r.inFlight.terminated {
		return
	}
	r.inFlight.terminated = true

	delete(r.state.inFlights, r.inFlight)
	if r.inFlight.reference != "" {
		delete(r.state.inFlightsByReference, r.inFlight.reference)
	}

	if r.inFlight.previous != nil {
		r.inFlight.previous.next = r.inFlight.next
	}
	if r.inFlight.next != nil {
		r.inFlight.next.previous = r.inFlight.previous
	}
	if r.state.moreRecentInFlight == r.inFlight {
		r.state.moreRecentInFlight = r.inFlight.previous
	}

	if transaction != nil && transaction.Timestamp.After(r.state.lastTransactionDate) {
		r.state.lastTransactionDate = transaction.Timestamp
	}
}
