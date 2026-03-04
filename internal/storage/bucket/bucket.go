package bucket

import (
	"context"
	"github.com/formancehq/go-libs/v3/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source bucket.go -destination bucket_generated_test.go -package bucket . Bucket
type Bucket interface {
	Migrate(ctx context.Context, db bun.IDB, opts ...migrations.Option) error
	AddLedger(ctx context.Context, db bun.IDB, ledger ledger.Ledger) error
	HasMinimalVersion(ctx context.Context, db bun.IDB) (bool, error)
	IsUpToDate(ctx context.Context, db bun.IDB) (bool, error)
	GetMigrationsInfo(ctx context.Context, db bun.IDB) ([]migrations.Info, error)
	IsInitialized(context.Context, bun.IDB) (bool, error)
	GetLastVersion(ctx context.Context, db bun.IDB) (int, error)
}

type Factory interface {
	Create(name string) Bucket
	GetMigrator(b string, db bun.IDB) *migrations.Migrator
}

type DefaultFactory struct {
	tracer trace.Tracer
}

func (f *DefaultFactory) Create(name string) Bucket {
	return NewDefault(f.tracer, name)
}

func (f *DefaultFactory) GetMigrator(b string, db bun.IDB) *migrations.Migrator {
	return GetMigrator(db, b)
}

func NewDefaultFactory(options ...DefaultFactoryOption) *DefaultFactory {
	ret := &DefaultFactory{}
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
