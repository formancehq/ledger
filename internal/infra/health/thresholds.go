package health

import "github.com/formancehq/ledger/v3/internal/domain"

// VolumeSample is one node's WAL and data usage fractions (0..1) for a single
// health-check cycle.
type VolumeSample struct {
	WALFraction  float64
	DataFraction float64
}

// Thresholds holds the block (high-water) and resume (low-water) marks for the
// WAL and data volumes. Each resume mark must be strictly positive and strictly
// below its block mark (0 < resume < block, enforced at startup config
// validation): a zero resume could never be cleared, since usage fractions are
// always >= 0.
type Thresholds struct {
	WALBlock   float64
	WALResume  float64
	DataBlock  float64
	DataResume float64
}

// anyAtBlock reports whether any sampled volume is at or above its block mark.
func (t Thresholds) anyAtBlock(samples []VolumeSample) bool {
	for _, s := range samples {
		if s.WALFraction >= t.WALBlock || s.DataFraction >= t.DataBlock {
			return true
		}
	}

	return false
}

// allBelowResume reports whether every sampled volume is strictly below its
// resume mark.
func (t Thresholds) allBelowResume(samples []VolumeSample) bool {
	for _, s := range samples {
		if s.WALFraction >= t.WALResume || s.DataFraction >= t.DataResume {
			return false
		}
	}

	return true
}

// NextDiskBlocked applies hysteresis: block when any volume hits its high-water
// mark; clear only when every volume drops below its low-water mark; otherwise
// hold the previous state (the band between resume and block).
func (t Thresholds) NextDiskBlocked(prev bool, samples []VolumeSample) bool {
	if len(samples) == 0 {
		// Defensive guard for the pure function: check() always appends the local
		// node's sample before calling this, so an empty slice does not occur on
		// the live path. With no measurements we can confirm neither a new problem
		// (needs a sample at/over block) nor a recovery (needs every sample below
		// resume), so hold the previous state.
		return prev
	}
	if !prev {
		return t.anyAtBlock(samples)
	}

	return !t.allBelowResume(samples)
}

// writeGateErrorForState selects the write-gate error for the current block
// state. Disk takes precedence over skew so the more actionable (back-off)
// signal surfaces first.
func writeGateErrorForState(diskBlocked, skewBlocked bool) error {
	switch {
	case diskBlocked:
		return domain.ErrWritesBlockedDiskFull
	case skewBlocked:
		return domain.ErrWritesBlockedClockSkew
	default:
		return nil
	}
}
