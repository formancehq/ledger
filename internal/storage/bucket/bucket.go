package bucket

import (
	"context"
	"github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
)

type Bucket interface {
	Migrate(ctx context.Context, tracer trace.Tracer, minimalVersionReached chan struct{}, opts ...migrations.Option) error
	AddLedger(ctx context.Context, ledger ledger.Ledger, db bun.IDB) error
	HasMinimalVersion(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
}

type Factory interface {
	Create(db *bun.DB, name string) Bucket
}

type DefaultFactory struct {}

func (f *DefaultFactory) Create(db *bun.DB, name string) Bucket {
	return NewDefault(db, name)
}

func NewDefaultFactory() *DefaultFactory {
	return &DefaultFactory{}
}