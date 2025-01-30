package leadership

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/uptrace/bun"
	"time"
)

type Manager struct {
	locker      Locker
	changes     *Broadcaster[Leadership]
	logger      logging.Logger
	retryPeriod time.Duration
	stopChannel chan chan struct{}
}

func (m *Manager) Run(ctx context.Context) {
	var (
		dbMutex   *DatabaseHandle
		nextRetry = time.After(time.Duration(0))
		nextPing  <-chan time.Time
	)
	for {
		select {
		case ch := <-m.stopChannel:
			if dbMutex != nil {
				m.logger.Info("leadership lost")
				dbMutex.Exec(func(_ bun.IDB) {
					_ = dbMutex.db.Close()
				})

				setIsLeader(ctx, false)
				m.changes.Broadcast(Leadership{})
			}
			close(ch)
			close(m.stopChannel)
			return
		case <-nextRetry:
			db, err := m.locker.Take(ctx)
			if err != nil || db == nil {
				if err != nil {
					m.logger.Error("error acquiring lock", err)
				}
				nextRetry = time.After(m.retryPeriod)
				continue
			}

			dbMutex = NewDatabaseHandle(db)

			m.changes.Broadcast(Leadership{
				DB:       dbMutex,
				Acquired: true,
			})
			m.logger.Info("leadership acquired")

			setIsLeader(ctx, true)

			nextPing = time.After(m.retryPeriod)

		// Ping the database to check the connection status
		// If the connection is lost, signal the listeners about the leadership loss
		case <-nextPing:
			dbMutex.Exec(func(db bun.IDB) {
				_, err := db.
					NewSelect().
					ColumnExpr("1 as v").
					Count(ctx)
				if err != nil {
					m.logger.Errorf("error pinging db: %s", err)
					_ = dbMutex.db.Close()
					dbMutex = nil

					setIsLeader(ctx, false)
					m.changes.Broadcast(Leadership{})

					nextRetry = time.After(m.retryPeriod)
				} else {
					nextPing = time.After(m.retryPeriod)
				}
			})
		}
	}
}

func (m *Manager) Stop(ctx context.Context) error {
	select {
	// if already closed
	case <-m.stopChannel:
		m.changes.Close()
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

func (m *Manager) GetBroadcaster() *Broadcaster[Leadership] {
	return m.changes
}

func NewManager(locker Locker, logger logging.Logger, options ...Option) *Manager {
	l := &Manager{
		locker:      locker,
		logger:      logger,
		changes:     NewBroadcaster[Leadership](),
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
