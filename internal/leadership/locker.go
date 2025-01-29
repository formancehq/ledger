package leadership

import (
	"context"
	"fmt"
	"github.com/uptrace/bun"
)

const leadershipAdvisoryLockKey = 123456789

type DBHandle interface {
	bun.IDB
	Close() error
}

// Locker take a lock at process level
// It returns a bun.IDB which MUST be invalidated when the lock is lost
type Locker interface {
	Take(ctx context.Context) (DBHandle, error)
}

type defaultLocker struct {
	db *bun.DB
}

func (p *defaultLocker) Take(ctx context.Context) (DBHandle, error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("error opening new connection: %w", err)
	}

	ret := conn.QueryRowContext(ctx, "select pg_try_advisory_lock(?)", leadershipAdvisoryLockKey)
	if ret.Err() != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("error acquiring lock: %w", ret.Err())
	}

	var acquired bool
	if err := ret.Scan(&acquired); err != nil {
		_ = conn.Close()
		panic(err)
	}

	if !acquired {
		_ = conn.Close()
		return nil, nil
	}

	return conn, nil
}

func NewDefaultLocker(db *bun.DB) Locker {
	return &defaultLocker{db: db}
}
