package system

import (
	"context"
	"reflect"
	"time"

	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"go.opentelemetry.io/otel/attribute"

	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

type Controller interface {
	GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error)
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error)
	// CreateLedger can return following errors:
	//  * ErrLedgerAlreadyExists
	//  * ledger.ErrInvalidLedgerName
	// It create the ledger in system store and the underlying storage
	CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error
	UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	MarkBucketAsDeleted(ctx context.Context, bucketName string) error
	RestoreBucket(ctx context.Context, bucketName string) error
	ListBucketsWithStatus(ctx context.Context) ([]BucketWithStatus, error)
}

type DefaultController struct {
	store    Store
	listener ledgercontroller.Listener
	// The numscript runtime used by default
	defaultParser ledgercontroller.NumscriptParser
	// The numscript runtime used when the "machine" runtime option is passed
	machineParser ledgercontroller.NumscriptParser
	// The numscript runtime used when the "interpreter" runtime option is passed
	interpreterParser          ledgercontroller.NumscriptParser
	registry                   *ledgercontroller.StateRegistry
	databaseRetryConfiguration DatabaseRetryConfiguration

	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	enableFeatures bool
}

func (ctrl *DefaultController) GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "GetLedgerController", func(ctx context.Context) (ledgercontroller.Controller, error) {
		store, l, err := ctrl.store.OpenLedger(ctx, name)
		if err != nil {
			return nil, err
		}

		instrumentationAttributes := []attribute.KeyValue{
			attribute.String("ledger", name),
		}

		meter := ctrl.meterProvider.Meter("ledger", metric.WithInstrumentationAttributes(
			instrumentationAttributes...,
		))
		tracer := ctrl.tracerProvider.Tracer("ledger", trace.WithInstrumentationAttributes(
			instrumentationAttributes...,
		))

		var ledgerController ledgercontroller.Controller = ledgercontroller.NewDefaultController(
			*l,
			store,
			ctrl.defaultParser,
			ctrl.machineParser,
			ctrl.interpreterParser,
			ledgercontroller.WithMeter(meter),
		)

		// Add too many client error handling
		ledgerController = ledgercontroller.NewControllerWithTooManyClientHandling(
			ledgerController,
			tracer,
			ledgercontroller.DelayCalculatorFn(func(i int) time.Duration {
				if i < ctrl.databaseRetryConfiguration.MaxRetry {
					return time.Duration(i+1) * ctrl.databaseRetryConfiguration.Delay
				}

				return 0
			}),
		)

		// Add cache regarding database state
		ledgerController = ledgercontroller.NewControllerWithCache(*l, ledgerController, ctrl.registry)

		// Add traces
		ledgerController = ledgercontroller.NewControllerWithTraces(ledgerController, tracer, meter)

		// Add events listener
		if ctrl.listener != nil {
			ledgerController = ledgercontroller.NewControllerWithEvents(*l, ledgerController, ctrl.listener)
		}

		return newLedgerStateTracker(ledgerController, *l), nil
	})
}

func (ctrl *DefaultController) CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "CreateLedger", tracing.NoResult(func(ctx context.Context) error {
		configuration.SetDefaults()

		if !ctrl.enableFeatures {
			if !reflect.DeepEqual(configuration.Features, features.DefaultFeatures) {
				return ErrExperimentalFeaturesDisabled
			}
		}

		l, err := ledger.New(name, configuration)
		if err != nil {
			return newErrInvalidLedgerConfiguration(err)
		}

		return ctrl.store.CreateLedger(ctx, l)
	})))
}

func (ctrl *DefaultController) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "GetLedger", func(ctx context.Context) (*ledger.Ledger, error) {
		return ctrl.store.GetLedger(ctx, name)
	})
}

func (ctrl *DefaultController) ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "ListLedgers", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Ledger], error) {
		return ctrl.store.ListLedgers(ctx, query)
	})
}

func (ctrl *DefaultController) UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "UpdateLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.UpdateLedgerMetadata(ctx, name, m)
	})))
}

func (ctrl *DefaultController) DeleteLedgerMetadata(ctx context.Context, param string, key string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "DeleteLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.DeleteLedgerMetadata(ctx, param, key)
	})))
}

func (ctrl *DefaultController) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "MarkBucketAsDeleted", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.MarkBucketAsDeleted(ctx, bucketName)
	})))
}

func (ctrl *DefaultController) RestoreBucket(ctx context.Context, bucketName string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "RestoreBucket", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.RestoreBucket(ctx, bucketName)
	})))
}

func (ctrl *DefaultController) ListBucketsWithStatus(ctx context.Context) ([]BucketWithStatus, error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "ListBucketsWithStatus", func(ctx context.Context) ([]BucketWithStatus, error) {
		return ctrl.store.ListBucketsWithStatus(ctx)
	})
}

func NewDefaultController(store Store, listener ledgercontroller.Listener, opts ...Option) *DefaultController {
	ret := &DefaultController{
		store:         store,
		listener:      listener,
		registry:      ledgercontroller.NewStateRegistry(),
		defaultParser: ledgercontroller.NewDefaultNumscriptParser(),
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type Option func(ctrl *DefaultController)

func WithParser(
	defaultParser ledgercontroller.NumscriptParser,
	machineParser ledgercontroller.NumscriptParser,
	interpreterParser ledgercontroller.NumscriptParser,
) Option {
	return func(ctrl *DefaultController) {
		ctrl.defaultParser = defaultParser
		ctrl.machineParser = machineParser
		ctrl.interpreterParser = interpreterParser
	}
}

func WithDatabaseRetryConfiguration(configuration DatabaseRetryConfiguration) Option {
	return func(ctrl *DefaultController) {
		ctrl.databaseRetryConfiguration = configuration
	}
}

func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(ctrl *DefaultController) {
		ctrl.meterProvider = mp
	}
}

func WithTracerProvider(t trace.TracerProvider) Option {
	return func(ctrl *DefaultController) {
		ctrl.tracerProvider = t
	}
}

func WithEnableFeatures(v bool) Option {
	return func(ctrl *DefaultController) {
		ctrl.enableFeatures = v
	}
}

var defaultOptions = []Option{
	WithMeterProvider(noopmetrics.MeterProvider{}),
	WithTracerProvider(nooptracer.TracerProvider{}),
}
