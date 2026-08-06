package readstore

import "sync"

// LeaseRegistry tracks the pinned sequences of live reads so the event GC
// (event_gc.go) never reclaims an event a pinned reader could still resolve
// (design sketch, see event_keys.go).
//
// A read registers its pinned sequence when it acquires its snapshots and
// releases on completion — for streaming reads, the lease must live as long
// as the cursor, not the handler call. The GC watermark is the minimum pinned
// sequence across live leases; with none, every fold-complete sequence is
// reclaimable.
type LeaseRegistry struct {
	mu     sync.Mutex
	nextID uint64
	leases map[uint64]uint64 // lease id -> pinned sequence
}

func NewLeaseRegistry() *LeaseRegistry {
	return &LeaseRegistry{leases: map[uint64]uint64{}}
}

// Lease is one live read's pin. Release is idempotent.
type Lease struct {
	r    *LeaseRegistry
	id   uint64
	once sync.Once
}

// Acquire registers a read pinned at seq.
func (r *LeaseRegistry) Acquire(seq uint64) *Lease {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	r.leases[r.nextID] = seq

	return &Lease{r: r, id: r.nextID}
}

func (l *Lease) Release() {
	l.once.Do(func() {
		l.r.mu.Lock()
		delete(l.r.leases, l.id)
		l.r.mu.Unlock()
	})
}

// Watermark returns the lowest pinned sequence across live leases; ok=false
// means no read is live and the caller may use its fold cursor instead.
func (r *LeaseRegistry) Watermark() (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	found := false

	var lowest uint64
	for _, seq := range r.leases {
		if !found || seq < lowest {
			lowest = seq
			found = true
		}
	}

	return lowest, found
}
