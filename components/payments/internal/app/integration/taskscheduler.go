package integration

import (
	"github.com/formancehq/payments/internal/app/task"
)

type TaskSchedulerFactory interface {
	Make(resolver task.Resolver, maxTasks int) *task.DefaultTaskScheduler
}

type TaskSchedulerFactoryFn func(resolver task.Resolver, maxProcesses int) *task.DefaultTaskScheduler

func (fn TaskSchedulerFactoryFn) Make(resolver task.Resolver, maxTasks int) *task.DefaultTaskScheduler {
	return fn(resolver, maxTasks)
}
