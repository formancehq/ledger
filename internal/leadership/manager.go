package leadership

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"time"
)

type Manager struct {
	locker      Locker
	changes     *Broadcaster
	logger      logging.Logger
	retryPeriod time.Duration
	stopChannel chan chan struct{}
}

func (m *Manager) Run(ctx context.Context) {
	var (
		db        DBHandle
		nextRetry = time.After(time.Duration(0))
		err       error
	)
	for {
		select {
		case ch := <-m.stopChannel:
			if db != nil {
				m.logger.Info("leadership lost")
				_ = db.Close()
				setIsLeader(ctx, false)
				m.changes.Broadcast(Leadership{})
			}
			close(ch)
			close(m.stopChannel)
			return
		case <-nextRetry:
			db, err = m.locker.Take(ctx)
			if err != nil || db == nil {
				if err != nil {
					m.logger.Error("error acquiring lock", err)
				}
				nextRetry = time.After(m.retryPeriod)
				continue
			}

			m.changes.Broadcast(Leadership{
				DB:       db,
				Acquired: true,
			})
			m.logger.Info("leadership acquired")

			setIsLeader(ctx, true)
		}
	}
}

func (m *Manager) Stop(ctx context.Context) error {
	select {
	// if already closed
	case <-m.stopChannel:
		return nil
	default:
		ch := make(chan struct{})
		m.stopChannel <- ch
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}
}

func (m *Manager) GetSignal() *Broadcaster {
	return m.changes
}

func NewManager(locker Locker, logger logging.Logger, options ...Option) *Manager {
	l := &Manager{
		locker:      locker,
		logger:      logger,
		changes:     NewSignal(),
		retryPeriod: 2 * time.Second,
		stopChannel: make(chan chan struct{}),
	}

	for _, option := range options {
		option(l)
	}

	return l
}

type Option func(leadership *Manager)

func WithRetryPeriod(duration time.Duration) Option {
	return func(leadership *Manager) {
		leadership.retryPeriod = duration
	}
}
