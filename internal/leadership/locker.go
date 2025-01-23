package leadership

import (
	"context"
	"fmt"
	"github.com/uptrace/bun"
)

const leadershipAdvisoryLockKey = 123456789

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source locker.go -destination locker_generated_test.go -package leadership . Locker
type Locker interface {
	Take(ctx context.Context) (bool, func(), error)
}

type defaultLocker struct {
	db *bun.DB
}

func (p *defaultLocker) Take(ctx context.Context) (bool, func(), error) {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("error opening new connection: %w", err)
	}

	ret := conn.QueryRowContext(ctx, "select pg_try_advisory_lock(?)", leadershipAdvisoryLockKey)
	if ret.Err() != nil {
		_ = conn.Close()
		return false, nil, fmt.Errorf("error acquiring lock: %w", ret.Err())
	}

	var acquired bool
	if err := ret.Scan(&acquired); err != nil {
		_ = conn.Close()
		panic(err)
	}

	if !acquired {
		_ = conn.Close()
		return false, nil, nil
	}

	return true, func() {
		_ = conn.Close()
	}, nil
}

func NewDefaultLocker(db *bun.DB) Locker {
	return &defaultLocker{db: db}
}
