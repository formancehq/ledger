package leadership

import "sync"

type Mutex[T any] struct {
	*sync.Mutex
	t T
}

func (m *Mutex[T]) Lock() (T, func()) {
	m.Mutex.Lock()
	return m.t, m.Unlock
}

func NewMutex[T any](t T) *Mutex[T] {
	return &Mutex[T]{
		Mutex: &sync.Mutex{},
		t:     t,
	}
}
