package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The restore cycle periodically hands the live ledger to an external
// orchestrator that backs it up, restores the backup into a fresh store (running
// the RebuildDelta replay), and brings a node back up on it. The driver keeps
// running against the restored node afterward, so its ordinary read/commit
// checks validate the rebuilt state — no separate comparison. The driver owns
// only the timing and the quiescence; the environment-specific work lives
// behind the file rendezvous — run_model_test.sh locally, the
// restore-orchestrator sidecar (driving the operator) on Antithesis k8s.

// awaitResume blocks while a restore cycle has paused dispatch, returning false
// if ctx ends. Safe to call without holding mu.
func (c *Checker) awaitResume(ctx context.Context) bool {
	for {
		c.mu.Lock()
		if !c.paused {
			c.mu.Unlock()
			return true
		}
		ch := c.resumeCh
		c.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
}

// pauseAndDrain stops new dispatch, then waits until every dispatched operation
// (in-flight bulk, outstanding read, buffered success) has drained — at which
// point modelState is the exact committed state the backup will capture. Returns
// false if ctx ends first. Pair with resume.
func (c *Checker) pauseAndDrain(ctx context.Context) bool {
	c.mu.Lock()
	if !c.paused {
		c.paused = true
		c.resumeCh = make(chan struct{})
	}
	c.mu.Unlock()

	for {
		c.mu.Lock()
		_, empty := c.earliestOutstanding()
		if empty {
			// Nothing is in flight; flush the re-order buffer. handleObservation's
			// failure/transient paths drop the ticket without calling tryDrain, so
			// with no further op to re-trigger it a committed success can sit here.
			c.tryDrain()
		}
		idle := empty && len(c.pending) == 0
		c.mu.Unlock()
		if idle {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(quiescePoll):
		}
	}
}

// resume releases workers parked in awaitResume.
func (c *Checker) resume() {
	c.mu.Lock()
	if c.paused {
		c.paused = false
		close(c.resumeCh)
	}
	c.mu.Unlock()
}

// RestoreTrigger fires one backup+restore cycle and returns once the ledger is
// serving again from restored state. Fire runs while the driver is quiesced.
type RestoreTrigger interface {
	Fire(ctx context.Context) error
}

// runRestoreCycle quiesces the driver, fires a restore, and resumes, on a jittered
// interval. A cycle that errors (e.g. an injected fault interrupted the backup) is
// logged and skipped: only wrong restored state — caught by the normal checks once
// the driver resumes against the restored node — is a finding. resume always runs
// so a stuck trigger cannot leave workers parked.
func runRestoreCycle(ctx context.Context, c *Checker, trigger RestoreTrigger, interval time.Duration) {
	for {
		jitter := time.Duration(internal.Rand().Int63n(int64(interval/2) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval + jitter):
		}

		log.Printf("restore cycle: quiescing")
		if !c.pauseAndDrain(ctx) {
			c.resume()
			return
		}

		log.Printf("restore cycle: drained, firing restore")
		err := func() error {
			defer c.resume()
			return trigger.Fire(ctx)
		}()
		if err != nil {
			log.Printf("restore cycle: %v (continuing)", err)
		} else {
			// The report-visible proof that restore coverage actually ran: a
			// green run with this never hit exercised nothing (the local
			// runner's zero-cycles guard, for Antithesis).
			assert.Sometimes(true, "singleton_driver_model: restore cycle completed", internal.Details{})
			log.Printf("restore cycle: complete, resumed")
		}
	}
}

// fileTrigger signals the local orchestrator (run_model_test.sh) through a
// request/response file rendezvous: it writes a request file, then waits for the
// orchestrator's response file. The orchestrator does the backup, restore, and
// node relaunch. A file rendezvous — not a socket — because the local orchestrator
// is bash, which polls files far more robustly than it speaks sockets.
type fileTrigger struct {
	reqPath  string
	respPath string
}

func (t *fileTrigger) Fire(ctx context.Context) error {
	_ = os.Remove(t.respPath)
	if err := os.WriteFile(t.reqPath, []byte("restore\n"), 0o600); err != nil {
		return fmt.Errorf("writing restore request: %w", err)
	}

	cap := restoreTimeout()
	timeout := time.NewTimer(cap)
	defer timeout.Stop()

	for {
		if data, err := os.ReadFile(t.respPath); err == nil {
			_ = os.Remove(t.respPath)
			_ = os.Remove(t.reqPath)
			if line := strings.TrimSpace(string(data)); line != "" && line != "ok" {
				return fmt.Errorf("orchestrator reported: %s", line)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("restore timed out after %s", cap)
		case <-time.After(restorePoll):
		}
	}
}

// restoreTimeout is the per-cycle lease, from MODEL_RESTORE_TIMEOUT seconds.
func restoreTimeout() time.Duration {
	return time.Duration(envInt("MODEL_RESTORE_TIMEOUT", defaultRestoreTimeoutSecs)) * time.Second
}

// selectRestoreTrigger returns the configured trigger, or nil when the restore
// cycle is not enabled for this run. Local runs set MODEL_RESTORE_REQ/RESP.
func selectRestoreTrigger() RestoreTrigger {
	if req, resp := os.Getenv("MODEL_RESTORE_REQ"), os.Getenv("MODEL_RESTORE_RESP"); req != "" && resp != "" {
		return &fileTrigger{reqPath: req, respPath: resp}
	}

	return nil
}

// restoreInterval is the base cycle interval, from MODEL_RESTORE_INTERVAL seconds.
func restoreInterval() time.Duration {
	return time.Duration(envInt("MODEL_RESTORE_INTERVAL", defaultRestoreIntervalSecs)) * time.Second
}
