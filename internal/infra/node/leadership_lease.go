package node

import (
	"context"
	"sync"
)

// LeadershipLease identifies one process-local Raft leadership tenure. Node
// alone issues and revokes leases. It is never serialized or persisted.
type LeadershipLease struct {
	mu     sync.Mutex
	active bool
	epoch  uint64
	term   uint64
	ctx    context.Context
	cancel context.CancelFunc
}

func newLeadershipLease(parent context.Context, epoch, term uint64) *LeadershipLease {
	ctx, cancel := context.WithCancel(parent)

	return &LeadershipLease{active: true, epoch: epoch, term: term, ctx: ctx, cancel: cancel}
}

// Epoch is a monotonic local transition identity. It is not a distributed
// fencing token.
func (l *LeadershipLease) Epoch() uint64 { return l.epoch }

// Context is cancelled synchronously when Node revokes this tenure.
func (l *LeadershipLease) Context() context.Context { return l.ctx }

// Authorize orders a short, non-blocking local registration or start against
// revocation. The callback must not wait, perform I/O, or drain goroutines.
func (l *LeadershipLease) Authorize(fn func()) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return false
	}
	fn()

	return true
}

func (l *LeadershipLease) revoke() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active {
		l.active = false
		l.cancel()
	}
}

type leadershipLeaseContextKey struct{}

// WithLeadershipLease carries the original tenure across slow preparation and
// through proposal admission. It does not change the caller's cancellation or
// value semantics; callers should separately derive runtime work from the lease.
func WithLeadershipLease(ctx context.Context, lease *LeadershipLease) context.Context {
	return context.WithValue(ctx, leadershipLeaseContextKey{}, lease)
}

func leaseFromContext(ctx context.Context) (*LeadershipLease, bool) {
	lease, ok := ctx.Value(leadershipLeaseContextKey{}).(*LeadershipLease)

	return lease, ok
}

// CurrentLeadershipLease returns the current tenure for new local work. Callers
// must retain this exact token and use Authorize for registration, or pass it
// with WithLeadershipLease for a later proposal. A snapshot alone is not an
// authorization.
func (node *Node) CurrentLeadershipLease() *LeadershipLease {
	node.leadershipMu.Lock()
	defer node.leadershipMu.Unlock()

	return node.leadershipLease
}

func (node *Node) gainLeadership(parent context.Context, term uint64) *LeadershipLease {
	node.leadershipMu.Lock()
	defer node.leadershipMu.Unlock()
	if node.leadershipClosed {
		return nil
	}
	node.leadershipEpoch++
	lease := newLeadershipLease(parent, node.leadershipEpoch, term)
	node.leadershipLease = lease

	return lease
}

func (node *Node) loseLeadership() (*LeadershipLease, uint64) {
	node.leadershipMu.Lock()
	defer node.leadershipMu.Unlock()
	old := node.leadershipLease
	node.leadershipLease = nil
	node.leadershipEpoch++
	old.revoke()

	return old, node.leadershipEpoch
}

func (node *Node) closeLeadership() {
	node.leadershipMu.Lock()
	defer node.leadershipMu.Unlock()
	node.leadershipClosed = true
	node.leadershipLease.revoke()
	node.leadershipLease = nil
}
