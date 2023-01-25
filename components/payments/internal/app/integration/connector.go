package integration

import (
	"context"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/task"
)

// Connector provide entry point to a payment provider.
type Connector interface {
	// Install is used to start the connector. The implementation if in charge of scheduling all required resources.
	Install(ctx task.ConnectorContext) error
	// Uninstall is used to uninstall the connector. It has to close all related resources opened by the connector.
	Uninstall(ctx context.Context) error
	// Resolve is used to recover state of a failed or restarted task
	Resolve(descriptor models.TaskDescriptor) task.Task
}

type ConnectorBuilder struct {
	name      string
	uninstall func(ctx context.Context) error
	resolve   func(descriptor models.TaskDescriptor) task.Task
	install   func(ctx task.ConnectorContext) error
}

func (b *ConnectorBuilder) WithUninstall(
	uninstallFunction func(ctx context.Context) error,
) *ConnectorBuilder {
	b.uninstall = uninstallFunction

	return b
}

func (b *ConnectorBuilder) WithResolve(resolveFunction func(name models.TaskDescriptor) task.Task) *ConnectorBuilder {
	b.resolve = resolveFunction

	return b
}

func (b *ConnectorBuilder) WithInstall(installFunction func(ctx task.ConnectorContext) error) *ConnectorBuilder {
	b.install = installFunction

	return b
}

func (b *ConnectorBuilder) Build() Connector {
	return &BuiltConnector{
		name:      b.name,
		uninstall: b.uninstall,
		resolve:   b.resolve,
		install:   b.install,
	}
}

func NewConnectorBuilder() *ConnectorBuilder {
	return &ConnectorBuilder{}
}

type BuiltConnector struct {
	name      string
	uninstall func(ctx context.Context) error
	resolve   func(name models.TaskDescriptor) task.Task
	install   func(ctx task.ConnectorContext) error
}

func (b *BuiltConnector) Name() string {
	return b.name
}

func (b *BuiltConnector) Install(ctx task.ConnectorContext) error {
	if b.install != nil {
		return b.install(ctx)
	}

	return nil
}

func (b *BuiltConnector) Uninstall(ctx context.Context) error {
	if b.uninstall != nil {
		return b.uninstall(ctx)
	}

	return nil
}

func (b *BuiltConnector) Resolve(name models.TaskDescriptor) task.Task {
	if b.resolve != nil {
		return b.resolve(name)
	}

	return nil
}

var _ Connector = &BuiltConnector{}
