package ledger

import (
	"context"
	"database/sql"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithCache struct {
	registry *StateRegistry
	ledger   ledger.Ledger
	Controller
}

func (c *ControllerWithCache) IsDatabaseUpToDate(ctx context.Context) (bool, error) {

	if c.registry.IsUpToDate(c.ledger.Name) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("cache-hit", true))
		return true, nil
	}

	upToDate, err := c.Controller.IsDatabaseUpToDate(ctx)
	if err != nil {
		return false, err
	}

	_ = c.registry.Upsert(c.ledger)
	if upToDate {
		c.registry.SetUpToDate(c.ledger.Name)
	}

	return upToDate, nil
}

func (c *ControllerWithCache) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, *bun.Tx, error) {
	ctrl, tx, err := c.Controller.BeginTX(ctx, options)
	if err != nil {
		return nil, nil, err
	}

	return &ControllerWithCache{
		registry:   c.registry,
		ledger:     c.ledger,
		Controller: ctrl,
	}, tx, nil
}

func (c *ControllerWithCache) LockLedger(ctx context.Context) (Controller, bun.IDB, func() error, error) {
	ctrl, db, release, err := c.Controller.LockLedger(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return &ControllerWithCache{
		registry:   c.registry,
		ledger:     c.ledger,
		Controller: ctrl,
	}, db, release, nil
}

func NewControllerWithCache(ledger ledger.Ledger, underlying Controller, registry *StateRegistry) *ControllerWithCache {
	return &ControllerWithCache{
		ledger:     ledger,
		Controller: underlying,
		registry:   registry,
	}
}
