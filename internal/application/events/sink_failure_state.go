package events

import (
	"math/rand/v2"
	"time"
)

// sinkFailureBackoffBase is the smallest delay applied after the first
// failure. Subsequent failures double up to sinkFailureBackoffCap. We
// start at 1s rather than the ticker's BatchDelay (10ms) so we don't
// spin on a broken sink even after the very first error.
const sinkFailureBackoffBase = 1 * time.Second

// sinkFailureBackoffCap is the maximum backoff between publish attempts
// while the sink is failing. 60s keeps recovery snappy enough for ops
// (a fixed sink shows healthy within a minute) while putting an upper
// bound on Raft churn under sustained failure.
const sinkFailureBackoffCap = 60 * time.Second

// sinkFailureRemindInterval is how often we re-propose an identical
// error message through Raft. Without this, a stuck broken sink would
// either freeze its SinkError.OccurredAt arbitrarily far in the past
// (if we only proposed on message change) or flood Raft (if we
// proposed on every failure). 5min is short enough for an alert to
// re-fire on a re-stamped timestamp, long enough to keep the Raft log
// quiet.
const sinkFailureRemindInterval = 5 * time.Minute

// sinkFailureState tracks per-sink failure bookkeeping for backoff and
// error-report deduplication. It is owned by a single Emitter goroutine
// — no synchronization.
type sinkFailureState struct {
	// consecutiveFails counts publish failures since the last success.
	// Reset to zero by recordSuccess.
	consecutiveFails int

	// nextRetryAt is the earliest wall-clock at which a new publish
	// attempt may run. Zero means "no backoff pending".
	nextRetryAt time.Time

	// lastReportedErr is the message of the most recent SinkError that
	// was proposed through Raft. Empty after recordSuccess.
	lastReportedErr string

	// lastReportedAt is when lastReportedErr was proposed. Drives the
	// remind interval.
	lastReportedAt time.Time
}

// shouldRetry reports whether a publish attempt may run at the given
// wall-clock. Returns true when no backoff is pending or when the
// backoff window has elapsed.
func (s *sinkFailureState) shouldRetry(now time.Time) bool {
	return s.nextRetryAt.IsZero() || !now.Before(s.nextRetryAt)
}

// recordFailure registers a publish failure and returns whether the
// caller should propose a fresh SinkError through Raft. The caller
// must respect the boolean — proposing on every call defeats the
// dedup.
//
// shouldReport is true when:
//   - this is the first failure (lastReportedErr empty), or
//   - the error message changed, or
//   - sinkFailureRemindInterval elapsed since the last report.
func (s *sinkFailureState) recordFailure(now time.Time, err error) (shouldReport bool) {
	s.consecutiveFails++
	s.nextRetryAt = now.Add(nextBackoff(s.consecutiveFails))

	msg := err.Error()

	switch {
	case s.lastReportedErr == "":
		shouldReport = true
	case s.lastReportedErr != msg:
		shouldReport = true
	case now.Sub(s.lastReportedAt) >= sinkFailureRemindInterval:
		shouldReport = true
	}

	if shouldReport {
		s.lastReportedErr = msg
		s.lastReportedAt = now
	}

	return shouldReport
}

// recordSuccess clears all failure bookkeeping. The next failure is
// treated as fresh: backoff restarts at the base, the error is
// reported even if the message matches the previously cleared one.
func (s *sinkFailureState) recordSuccess() {
	s.consecutiveFails = 0
	s.nextRetryAt = time.Time{}
	s.lastReportedErr = ""
	s.lastReportedAt = time.Time{}
}

// nextBackoff returns the wait duration after the n-th consecutive
// failure (n >= 1). Exponential doubling from sinkFailureBackoffBase,
// capped at sinkFailureBackoffCap, with ±25% jitter applied last so
// that fleets recovering at the same time don't synchronize.
func nextBackoff(consecutiveFails int) time.Duration {
	if consecutiveFails < 1 {
		consecutiveFails = 1
	}

	d := sinkFailureBackoffBase

	for i := 1; i < consecutiveFails; i++ {
		d *= 2
		if d >= sinkFailureBackoffCap {
			d = sinkFailureBackoffCap

			break
		}
	}

	// Jitter: ±25% of d. rand.Float64() ∈ [0, 1), so (2*r - 1) ∈ [-1, 1).
	jitterFactor := (rand.Float64()*2 - 1) * 0.25

	return d + time.Duration(float64(d)*jitterFactor)
}
