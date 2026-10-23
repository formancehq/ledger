package controller

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	claimTemplateRetryInitial   = 2 * time.Second
	claimTemplateRetryMaximum   = 30 * time.Second
	claimTemplateRetryRetention = 10 * time.Minute
)

// claimTemplateRecoveryRetryWait means this pass attempted no durable recovery
// write. The caller must still directly fence Cluster identity and nil protected
// status before continuing independently safe mutable work.
type claimTemplateRecoveryRetryWait struct{ after time.Duration }

func (e *claimTemplateRecoveryRetryWait) Error() string {
	return fmt.Sprintf("storage recovery preparation retry is scheduled in %s", e.after)
}

type claimTemplateRetryState struct {
	uid         types.UID
	digest      string
	delay       time.Duration
	retryAt     time.Time
	lastFailure time.Time
}

// claimTemplateRetries limits failed preparation writes across watch wakeups.
// It is only a rate control: protected API status remains the sole authority for
// recovery. Restart or expiry after ten minutes without failure may reset this
// soft limit. There is one entry per Cluster name, not per historical payload;
// deletion removes it, and expiry never removes an active retry deadline.
type claimTemplateRetries struct {
	mu      sync.Mutex
	entries map[types.NamespacedName]claimTemplateRetryState
}

func (r *claimTemplateRetries) wait(ledger *ledgerv1alpha1.Cluster, digest string, now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire(now)
	key := client.ObjectKeyFromObject(ledger)
	state, exists := r.entries[key]
	if !exists {
		return 0
	}
	if state.uid != ledger.UID || state.digest != digest {
		delete(r.entries, key)

		return 0
	}

	return max(0, state.retryAt.Sub(now))
}

func (r *claimTemplateRetries) failed(ledger *ledgerv1alpha1.Cluster, digest string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expire(now)
	if r.entries == nil {
		r.entries = make(map[types.NamespacedName]claimTemplateRetryState)
	}
	key := client.ObjectKeyFromObject(ledger)
	state, exists := r.entries[key]
	delay := claimTemplateRetryInitial
	if exists && state.uid == ledger.UID && state.digest == digest {
		delay = min(state.delay*2, claimTemplateRetryMaximum)
	}
	r.entries[key] = claimTemplateRetryState{
		uid: ledger.UID, digest: digest, delay: delay,
		retryAt: now.Add(delay), lastFailure: now,
	}
}

func (r *claimTemplateRetries) succeeded(ledger *ledgerv1alpha1.Cluster, digest string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := client.ObjectKeyFromObject(ledger)
	if state, exists := r.entries[key]; exists && state.uid == ledger.UID && state.digest == digest {
		delete(r.entries, key)
	}
}

func (r *claimTemplateRetries) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, key)
}

// expire runs only under mu. Memory scales with recent failed Clusters, and
// each scan with that same cardinality. Retention exceeds the maximum delay.
func (r *claimTemplateRetries) expire(now time.Time) {
	for key, state := range r.entries {
		if now.Sub(state.lastFailure) >= claimTemplateRetryRetention {
			delete(r.entries, key)
		}
	}
}
