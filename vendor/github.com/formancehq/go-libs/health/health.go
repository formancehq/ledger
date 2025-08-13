package health

import "context"

type Check interface {
	Do(ctx context.Context) error
}
type CheckFn func(ctx context.Context) error

func (fn CheckFn) Do(ctx context.Context) error {
	return fn(ctx)
}

type NamedCheck interface {
	Check
	Name() string
}

type simpleNamedCheck struct {
	Check
	name string
}

func (c *simpleNamedCheck) Name() string {
	return c.name
}

func NewNamedCheck(name string, check Check) *simpleNamedCheck {
	return &simpleNamedCheck{
		Check: check,
		name:  name,
	}
}
