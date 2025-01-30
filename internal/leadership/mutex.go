package leadership

import (
	"github.com/uptrace/bun"
	"sync"
)

type Mutex struct {
	*sync.Mutex
	db DBHandle
}

func (m *Mutex) Exec(fn func(db bun.IDB)) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	fn(m.db)
}

func NewMutex(db DBHandle) *Mutex {
	return &Mutex{
		Mutex: &sync.Mutex{},
		db:    db,
	}
}
