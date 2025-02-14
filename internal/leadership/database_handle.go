package leadership

import (
	"github.com/uptrace/bun"
	"sync"
)

type DatabaseHandle struct {
	*sync.Mutex
	db DBHandle
}

func (m *DatabaseHandle) Exec(fn func(db bun.IDB)) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	fn(m.db)
}

func NewDatabaseHandle(db DBHandle) *DatabaseHandle {
	return &DatabaseHandle{
		Mutex: &sync.Mutex{},
		db:    db,
	}
}
