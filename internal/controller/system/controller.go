package system

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/formancehq/ledger/pkg/features"
)

type ReplicationBackend interface {
	ListExporters(ctx context.Context) (*bunpaginate.Cursor[ledger.Exporter], error)
	CreateExporter(ctx context.Context, configuration ledger.ExporterConfiguration) (*ledger.Exporter, error)
	UpdateExporter(ctx context.Context, id string, configuration ledger.ExporterConfiguration) error
	DeleteExporter(ctx context.Context, id string) error
	GetExporter(ctx context.Context, id string) (*ledger.Exporter, error)

	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error)
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
	CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error)
	DeletePipeline(ctx context.Context, id string) error
	StartPipeline(ctx context.Context, id string) error
	ResetPipeline(ctx context.Context, id string) error
	StopPipeline(ctx context.Context, id string) error
}

type Controller interface {
	ReplicationBackend
	GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error)
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error)
	// CreateLedger can return following errors:
	//  * ErrLedgerAlreadyExists
	//  * ledger.ErrInvalidLedgerName
	// It create the ledger in system store and the underlying storage
	CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error
	UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	DeleteBucket(ctx context.Context, bucket string) error
	RestoreBucket(ctx context.Context, bucket string) error
}

type DefaultController struct {
	driver   Driver
	listener ledgercontroller.Listener
	// The numscript runtime used by default
	defaultParser ledgercontroller.NumscriptParser
	// The numscript runtime used when the "machine" runtime option is passed
	machineParser ledgercontroller.NumscriptParser
	// The numscript runtime used when the "interpreter" runtime option is passed
	interpreterParser          ledgercontroller.NumscriptParser
	registry                   *ledgercontroller.StateRegistry
	databaseRetryConfiguration DatabaseRetryConfiguration
	replicationBackend         ReplicationBackend

	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	enableFeatures bool
}

func (ctrl *DefaultController) ListExporters(ctx context.Context) (*bunpaginate.Cursor[ledger.Exporter], error) {
	return ctrl.replicationBackend.ListExporters(ctx)
}

// CreateExporter can return following errors:
// * ErrInvalidDriverConfiguration
func (ctrl *DefaultController) CreateExporter(ctx context.Context, configuration ledger.ExporterConfiguration) (*ledger.Exporter, error) {
	ret, err := ctrl.replicationBackend.CreateExporter(ctx, configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}
	return ret, nil
}

// UpdateExporter can return following errors:
// * ErrInvalidDriverConfiguration
// * ErrExporterNotFound
func (ctrl *DefaultController) UpdateExporter(ctx context.Context, id string, configuration ledger.ExporterConfiguration) error {
	return ctrl.replicationBackend.UpdateExporter(ctx, id, configuration)
}

// DeleteExporter can return following errors:
// ErrExporterNotFound
func (ctrl *DefaultController) DeleteExporter(ctx context.Context, id string) error {
	return ctrl.replicationBackend.DeleteExporter(ctx, id)
}

// GetExporter can return following errors:
// ErrExporterNotFound
func (ctrl *DefaultController) GetExporter(ctx context.Context, id string) (*ledger.Exporter, error) {
	return ctrl.replicationBackend.GetExporter(ctx, id)
}

func (ctrl *DefaultController) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	return ctrl.replicationBackend.ListPipelines(ctx)
}

func (ctrl *DefaultController) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	return ctrl.replicationBackend.GetPipeline(ctx, id)
}

func (ctrl *DefaultController) CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	return ctrl.replicationBackend.CreatePipeline(ctx, pipelineConfiguration)
}

func (ctrl *DefaultController) DeletePipeline(ctx context.Context, id string) error {
	return ctrl.replicationBackend.DeletePipeline(ctx, id)
}

func (ctrl *DefaultController) StartPipeline(ctx context.Context, id string) error {
	return ctrl.replicationBackend.StartPipeline(ctx, id)
}

func (ctrl *DefaultController) ResetPipeline(ctx context.Context, id string) error {
	return ctrl.replicationBackend.ResetPipeline(ctx, id)
}

func (ctrl *DefaultController) StopPipeline(ctx context.Context, id string) error {
	return ctrl.replicationBackend.StopPipeline(ctx, id)
}

func (ctrl *DefaultController) GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "GetLedgerController", func(ctx context.Context) (ledgercontroller.Controller, error) {
		store, l, err := ctrl.driver.OpenLedger(ctx, name)
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

		return ctrl.driver.CreateLedger(ctx, l)
	})))
}

func (ctrl *DefaultController) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "GetLedger", func(ctx context.Context) (*ledger.Ledger, error) {
		return ctrl.driver.GetSystemStore().GetLedger(ctx, name)
	})
}

func (ctrl *DefaultController) ListLedgers(ctx context.Context, query common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "ListLedgers", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Ledger], error) {
		return ctrl.driver.GetSystemStore().Ledgers().Paginate(ctx, query)
	})
}

func (ctrl *DefaultController) UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "UpdateLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.driver.GetSystemStore().UpdateLedgerMetadata(ctx, name, m)
	})))
}

func (ctrl *DefaultController) DeleteLedgerMetadata(ctx context.Context, param string, key string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "DeleteLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.driver.GetSystemStore().DeleteLedgerMetadata(ctx, param, key)
	})))
}

func (ctrl *DefaultController) DeleteBucket(ctx context.Context, bucket string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "DeleteBucket", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.driver.GetSystemStore().DeleteBucket(ctx, bucket)
	})))
}

func (ctrl *DefaultController) RestoreBucket(ctx context.Context, bucket string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracerProvider.Tracer("system"), "RestoreBucket", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.driver.GetSystemStore().RestoreBucket(ctx, bucket)
	})))
}

// NewDefaultController creates a DefaultController configured with the provided
// store, listener, replication backend, and optional functional options.
//
// The controller is initialized with a new StateRegistry and a default Numscript
// parser; any of these defaults (and other fields) can be overridden by passing
// Option values. The returned controller is ready for further initialization or use.
func NewDefaultController(
	store Driver,
	listener ledgercontroller.Listener,
	replicationBackend ReplicationBackend,
	opts ...Option,
) *DefaultController {
	ret := &DefaultController{
		driver:             store,
		listener:           listener,
		registry:           ledgercontroller.NewStateRegistry(),
		defaultParser:      ledgercontroller.NewDefaultNumscriptParser(),
		replicationBackend: replicationBackend,
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
