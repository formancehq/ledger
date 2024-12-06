package bucket

import (
	"context"
	"github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Bucket interface {
	Migrate(ctx context.Context, opts ...migrations.Option) error
	AddLedger(ctx context.Context, ledger ledger.Ledger) error
	HasMinimalVersion(ctx context.Context) (bool, error)
	IsUpToDate(ctx context.Context) (bool, error)
	GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error)
	IsInitialized(context.Context) (bool, error)
}

type Factory interface {
	Create(name string) Bucket
	GetMigrator(b string) *migrations.Migrator
}

type DefaultFactory struct {
	tracer trace.Tracer
	db     *bun.DB
}

func (f *DefaultFactory) Create(name string) Bucket {
	return NewDefault(f.db, f.tracer, name)
}

func (f *DefaultFactory) GetMigrator(b string) *migrations.Migrator {
	return GetMigrator(f.db, b)
}

func NewDefaultFactory(db *bun.DB, options ...DefaultFactoryOption) *DefaultFactory {
	ret := &DefaultFactory{
		db: db,
	}
	for _, option := range append(defaultOptions, options...) {
		option(ret)
	}
	return ret
}

type DefaultFactoryOption func(factory *DefaultFactory)

func WithTracer(tracer trace.Tracer) DefaultFactoryOption {
	return func(factory *DefaultFactory) {
		factory.tracer = tracer
	}
}

var defaultOptions = []DefaultFactoryOption{
	WithTracer(noop.Tracer{}),
}
