package ledger

import (
	"sync"

	ledger "github.com/formancehq/ledger/internal"
)

type State struct {
	bucket   string
	upToDate bool
}

type StateRegistry struct {
	mu      sync.Mutex
	ledgers map[string]*State
}

func (r *StateRegistry) Upsert(l ledger.Ledger) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.ledgers[l.Name]; !ok {
		r.ledgers[l.Name] = &State{
			bucket: l.Bucket,
		}
		return true
	}
	return false
}

func (r *StateRegistry) SetUpToDate(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ledgers[name].upToDate = true
}

func (r *StateRegistry) IsUpToDate(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	l, ok := r.ledgers[name]
	if !ok {
		return false
	}

	return l.upToDate
}

func NewStateRegistry() *StateRegistry {
	return &StateRegistry{
		ledgers: make(map[string]*State),
	}
}
