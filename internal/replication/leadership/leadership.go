package leadership

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/replication/signal"
	"github.com/uptrace/bun"
	"time"
)

const leadershipAdvisoryLockKey = 123456789

type Leadership struct {
	db      *bun.DB
	changes *signal.Signal[bool]
	logger  logging.Logger
}

func (l *Leadership) acquire(ctx context.Context) error {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error opening new connection: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	ret := conn.QueryRowContext(ctx, "select pg_try_advisory_lock(?)", leadershipAdvisoryLockKey)
	if ret.Err() != nil {
		return fmt.Errorf("error acquiring lock: %w", ret.Err())
	}

	var acquired bool
	if err := ret.Scan(&acquired); err != nil {
		panic(err)
	}

	if acquired {
		l.changes.Signal(true)
		l.logger.Info("leadership acquired")
		<-ctx.Done()
		l.changes.Signal(false)
		return ctx.Err()
	} else {
		_ = conn.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
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

func (l *Leadership) GetLeadership() *signal.Signal[bool] {
	return l.changes
}

func NewLeadership(db *bun.DB, logger logging.Logger) *Leadership {
	return &Leadership{
		db:      db,
		logger:  logger,
		changes: signal.NewSignal(pointer.For(false)),
	}
}
