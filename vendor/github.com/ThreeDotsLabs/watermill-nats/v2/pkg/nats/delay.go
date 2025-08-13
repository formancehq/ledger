package nats

import (
	"time"
)

// TermSignal if this duration was returned, event will be term`ed
const TermSignal = time.Duration(-1)

type Delay interface {
	// WaitTime return time.Duration that we need to wait.
	// retryNum is how many times WaitTime was called for
	// specific message
	WaitTime(retryNum uint64) time.Duration
}

var _ Delay = (*StaticDelay)(nil)

// StaticDelay delay that always return the same time.Duration
type StaticDelay struct {
	Delay time.Duration
}

func NewStaticDelay(delay time.Duration) StaticDelay {
	return StaticDelay{Delay: delay}
}

func (s StaticDelay) WaitTime(retryNum uint64) time.Duration {
	return s.Delay
}

var _ Delay = (*MaxRetryDelay)(nil)

// MaxRetryDelay delay that returns the same time.Duration up to a maximum before sending term
type MaxRetryDelay struct {
	StaticDelay
	maxRetries uint64
}

func NewMaxRetryDelay(delay time.Duration, retryLimit uint64) MaxRetryDelay {
	return MaxRetryDelay{
		StaticDelay: NewStaticDelay(delay),
		maxRetries:  retryLimit,
	}
}

func (s MaxRetryDelay) WaitTime(retryNum uint64) time.Duration {
	if retryNum >= s.maxRetries {
		return TermSignal
	}
	return s.Delay
}
