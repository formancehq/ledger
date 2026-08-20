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
// sweep with, and Pin refuses any sequence below it.
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

// Reserve holds reclamation at seq for a reader that does not yet know its
// pin, and must be taken BEFORE the main-store handle is opened: the pin does
// not exist until the handle is open, so no lease can protect it, and under
// load the fold — and with it the reclaim floor — passes a just-opened handle
// within a tick.
//
// Callers pass the read index's current fold cursor. The fold reads from the
// main log and records its cursor only for logs already consumed, so the
// cursor is at or below the main store's applied sequence at that instant;
// the handle opens later and the applied sequence only moves forward, so the
// pin that follows is at or above the reserved sequence and its Pin cannot be
// refused. Reserving at the cursor rather than at the floor keeps the hold
// shallow: everything below the cursor stays reclaimable while the read waits
// for alignment.
//
// A sequence beneath the floor is raised to it — history below the floor may
// already be gone, so pinning there would claim a protection the registry
// cannot give.
func (r *LeaseRegistry) Reserve(seq uint64) *Lease {
	r.mu.Lock()
	defer r.mu.Unlock()

	if seq < r.floor {
		seq = r.floor
	}

	r.nextID++
	r.leases[r.nextID] = seq

	return &Lease{r: r, id: r.nextID}
}

// Pin registers a read at seq — the sequence the event GC must not reclaim
// past for as long as the returned lease is held. Registration is the point:
// the lease is how a live read enters the minimum BeginGC sweeps with, and
// without it a pass would be free to collapse groups the read is still
// resolving.
//
// ok=false means a GC pass has already begun reclaiming at or above seq, so
// events the read needs may be gone. It is unreachable for a caller holding a
// Reserve taken before its handle, and is a detector for one that opened a
// handle without it — not a condition reads are expected to meet.
func (r *LeaseRegistry) Pin(seq uint64) (*Lease, bool) {
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
