package node

import (
	"context"
	"reflect"
	"runtime/debug"

	collectionutils "github.com/formancehq/go-libs/v5/pkg/types/collections"
)

type task struct {
	error       chan error
	stopChannel chan struct{}
	fn          func(context.Context, chan struct{}) error
}

func (t *task) err() chan error {
	return t.error
}

func (t *task) run(ctx context.Context) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				t.error <- newPanickedError(e, debug.Stack())
			}
		}()

		t.error <- t.fn(ctx, t.stopChannel)
	}()
}

func (t *task) interrupt() {
	close(t.stopChannel)
}

func newTask(fn func(context.Context, chan struct{}) error) *task {
	return &task{
		error:       make(chan error, 1),
		fn:          fn,
		stopChannel: make(chan struct{}),
	}
}

type taskSet struct {
	tasks      []*task
	errChannel chan error
	terminated chan struct{}
}

func (pool *taskSet) add(t *task) {
	pool.tasks = append(pool.tasks, t)
}

func (pool *taskSet) run(ctx context.Context) {
	for _, t := range pool.tasks {
		t.run(ctx)
	}

	pool.errChannel = make(chan error, len(pool.tasks))

	go func() {
		defer close(pool.terminated)

		for {
			selectCases := collectionutils.Map(pool.tasks, func(t *task) reflect.SelectCase {
				return reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(t.err()),
				}
			})
			chosen, recv, _ := reflect.Select(selectCases)

			var err error
			if recv.IsNil() {
				err = nil
			} else {
				err = recv.Interface().(error)
			}

			pool.errChannel <- err

			pool.tasks = append(pool.tasks[:chosen], pool.tasks[chosen+1:]...)

			if len(pool.tasks) == 0 {
				return
			}
		}
	}()
}

func (pool *taskSet) err() chan error {
	return pool.errChannel
}

func (pool *taskSet) stop() error {
	for _, t := range pool.tasks {
		t.interrupt()
	}

	<-pool.terminated

	return nil
}

func newTaskSet() *taskSet {
	return &taskSet{
		tasks:      make([]*task, 0),
		terminated: make(chan struct{}),
	}
}
