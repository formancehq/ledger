package task

import "github.com/formancehq/payments/internal/app/models"

type Resolver interface {
	Resolve(descriptor models.TaskDescriptor) Task
}

type ResolverFn func(descriptor models.TaskDescriptor) Task

func (fn ResolverFn) Resolve(descriptor models.TaskDescriptor) Task {
	return fn(descriptor)
}
