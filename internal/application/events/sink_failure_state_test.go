package events

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSinkFailureState_FirstFailureReportsAndSetsBackoff(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	t0 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	require.True(t, s.shouldRetry(t0), "no backoff yet, should retry")

	shouldReport := s.recordFailure(t0, errors.New("boom"))
	require.True(t, shouldReport, "first failure must report")

	require.Equal(t, 1, s.consecutiveFails)
	require.True(t, s.nextRetryAt.After(t0), "nextRetryAt must be in the future after a failure")
	require.False(t, s.shouldRetry(t0), "must wait for backoff before retrying")
	require.True(t, s.shouldRetry(t0.Add(2*sinkFailureBackoffCap)),
		"after sufficient wait, retry must be allowed")
}

func TestSinkFailureState_DedupsIdenticalErrors(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	t0 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	err := errors.New("connection refused")

	require.True(t, s.recordFailure(t0, err), "first failure reports")

	// Same message within the remind interval: must NOT report.
	for i := range 10 {
		now := t0.Add(time.Duration(i+1) * 10 * time.Second)
		require.False(t, s.recordFailure(now, err),
			"identical error within remind interval must dedup (iter %d)", i)
	}
}

func TestSinkFailureState_ReportsOnMessageChange(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	t0 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	require.True(t, s.recordFailure(t0, errors.New("connection refused")))
	require.False(t, s.recordFailure(t0.Add(time.Second), errors.New("connection refused")),
		"same message must dedup")
	require.True(t, s.recordFailure(t0.Add(2*time.Second), errors.New("timeout")),
		"different message must report")
	require.False(t, s.recordFailure(t0.Add(3*time.Second), errors.New("timeout")),
		"same new message must dedup again")
}

func TestSinkFailureState_RemindAfterInterval(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	t0 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	err := errors.New("still broken")

	require.True(t, s.recordFailure(t0, err))

	// Just before the remind interval: still dedup.
	require.False(t,
		s.recordFailure(t0.Add(sinkFailureRemindInterval-time.Second), err),
		"before remind interval must dedup")

	// At or past the remind interval: report again.
	require.True(t,
		s.recordFailure(t0.Add(sinkFailureRemindInterval), err),
		"at remind interval must report")
}

func TestSinkFailureState_SuccessResetsEverything(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	t0 := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	for range 5 {
		s.recordFailure(t0, errors.New("boom"))
	}

	require.Equal(t, 5, s.consecutiveFails)
	require.NotEmpty(t, s.lastReportedErr)
	require.False(t, s.nextRetryAt.IsZero())

	s.recordSuccess()

	require.Equal(t, 0, s.consecutiveFails)
	require.Empty(t, s.lastReportedErr)
	require.True(t, s.nextRetryAt.IsZero())
	require.True(t, s.shouldRetry(t0))

	// Next failure with the previously-reported message must report
	// fresh, not be deduped against the cleared state.
	require.True(t, s.recordFailure(t0.Add(time.Second), errors.New("boom")),
		"failure after success must report fresh")
}

func TestNextBackoff_ExponentialAndCapped(t *testing.T) {
	t.Parallel()

	// With ±25% jitter, the i-th attempt's expected value is
	// base * 2^(i-1), capped at sinkFailureBackoffCap. Bound checks
	// assert we're in [0.75 * expected, 1.25 * expected].
	cases := []struct {
		n      int
		expect time.Duration
	}{
		{1, sinkFailureBackoffBase},      // 1s
		{2, 2 * sinkFailureBackoffBase},  // 2s
		{3, 4 * sinkFailureBackoffBase},  // 4s
		{6, 32 * sinkFailureBackoffBase}, // 32s — under cap
		{10, sinkFailureBackoffCap},      // capped
		{100, sinkFailureBackoffCap},     // capped
	}

	for _, c := range cases {
		// Sample multiple times to cover jitter range.
		for range 20 {
			got := nextBackoff(c.n)
			lo := time.Duration(float64(c.expect) * 0.75)
			hi := time.Duration(float64(c.expect) * 1.25)

			require.GreaterOrEqual(t, got, lo,
				"n=%d backoff %s below lower bound %s", c.n, got, lo)
			require.LessOrEqual(t, got, hi,
				"n=%d backoff %s above upper bound %s", c.n, got, hi)
		}
	}
}

// TestSinkFailureState_ProposalCountIsBounded sanity-checks the
// composition: simulating one tick every 10ms over 60s on a sink
// that always fails with the same message, we expect O(log) reports,
// not 6000. With base=1s, cap=60s, this is at most ~7 reports
// (1s, 2s, 4s, 8s, 16s, 32s, 60s) plus jitter, so we just assert
// "far fewer than ticks".
func TestSinkFailureState_ProposalCountIsBounded(t *testing.T) {
	t.Parallel()

	var s sinkFailureState
	err := errors.New("sink down")
	start := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)

	reports := 0
	ticks := 0

	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Millisecond {
		ticks++

		now := start.Add(offset)
		if !s.shouldRetry(now) {
			continue
		}

		if s.recordFailure(now, err) {
			reports++
		}
	}

	require.Greater(t, ticks, 1000, "sanity: we should have many ticks")
	require.Less(t, reports, 20,
		"a persistently failing sink must produce far fewer reports than ticks (got %d reports over %d ticks)",
		reports, ticks)
}
