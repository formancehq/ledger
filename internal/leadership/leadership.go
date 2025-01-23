package leadership

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"time"
)

type Leadership struct {
	locker      Locker
	changes     *Signal
	logger      logging.Logger
	retryPeriod time.Duration
}

func (l *Leadership) acquire(ctx context.Context) error {

	acquired, release, err := l.locker.Take(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring lock: %w", err)
	}

	if acquired {
		l.changes.Signal(true)
		l.logger.Info("leadership acquired")
		<-ctx.Done()
		l.logger.Info("leadership lost")
		release()
		l.changes.Signal(false)
		return ctx.Err()
	} else {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.retryPeriod):
		}
	}

	return nil
}

func (l *Leadership) Run(ctx context.Context) {
	for {
		if err := l.acquire(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			l.logger.Errorf("error acquiring leadership: %s", err)
		}
	}
}

func (l *Leadership) GetSignal() *Signal {
	return l.changes
}

func NewLeadership(locker Locker, logger logging.Logger, options ...Option) *Leadership {
	l := &Leadership{
		locker:      locker,
		logger:      logger,
		changes:     NewSignal(),
		retryPeriod: 2 * time.Second,
	}

	for _, option := range options {
		option(l)
	}

	return l
}

type Option func(leadership *Leadership)

func WithRetryPeriod(duration time.Duration) Option {
	return func(leadership *Leadership) {
		leadership.retryPeriod = duration
	}
}
