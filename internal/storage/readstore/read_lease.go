package readstore

import "sync"

// LeaseRegistry tracks the pinned sequences of live reads so the event GC
// (event_gc.go) never reclaims an event a pinned reader could still resolve
// (see event_keys.go).
//
// A read registers its pinned sequence when it acquires its snapshots and
// releases on completion — for streaming reads, the lease must live as long
// as the cursor, not the handler call. The GC watermark is the minimum pinned
// sequence across live leases; with none, the caller falls back to its fold
// cursor.
//
// A pin is chosen (ReadLastSequence) before it is registered, and a GC pass
// that samples the watermark in that gap would be free to reclaim beneath a
// pin that is about to be used. The registry therefore also publishes a
// monotone reclaim floor: BeginGC records the watermark a pass is about to
// sweep with, and Acquire refuses any pin below it. A refused reader re-reads
// its handle and retries rather than resolving a truncated group.
type LeaseRegistry struct {
	mu     sync.Mutex
	nextID uint64
	leases map[uint64]uint64 // lease id -> pinned sequence
	floor  uint64            // highest watermark any GC pass has begun with
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

// Acquire registers a read pinned at seq. ok=false means a GC pass has already
// begun reclaiming at or above seq, so events the pin needs may be gone; the
// caller must re-pin against fresher state instead of reading at seq.
func (r *LeaseRegistry) Acquire(seq uint64) (*Lease, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if seq < r.floor {
		return nil, false
	}

	r.nextID++
	r.leases[r.nextID] = seq

	return &Lease{r: r, id: r.nextID}, true
}

func (l *Lease) Release() {
	if l == nil {
		return
	}

	l.once.Do(func() {
		l.r.mu.Lock()
		delete(l.r.leases, l.id)
		l.r.mu.Unlock()
	})
}

// BeginGC reserves a watermark for a GC pass and returns the value the pass
// must actually sweep with — the lower of the requested watermark and every
// live pin, so a reader registered before the call keeps its history.
//
// Raising the floor and reading the live pins happen under one lock: a reader
// racing this call either registers first (and is covered by the returned
// watermark) or arrives after (and is refused by Acquire). Publishing the
// floor after the sweep would reopen exactly that gap.
func (r *LeaseRegistry) BeginGC(watermark uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, seq := range r.leases {
		if seq < watermark {
			watermark = seq
		}
	}

	if watermark > r.floor {
		r.floor = watermark
	}

	return watermark
}

// ReclaimFloor is the highest watermark any GC pass has begun with — the
// sequence below which history may already be gone.
func (r *LeaseRegistry) ReclaimFloor() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.floor
}
