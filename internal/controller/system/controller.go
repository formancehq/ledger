package system

import (
	"context"
	"database/sql"
	"errors"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
	"go.opentelemetry.io/otel/attribute"
	"reflect"
	"time"

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
	ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error)
	// CreateLedger can return following errors:
	//  * ErrLedgerAlreadyExists
	//  * ledger.ErrInvalidLedgerName
	// It create the ledger in system store and the underlying storage
	CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error
	UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error

	ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error)
	CreateConnector(ctx context.Context, configuration ledger.ConnectorConfiguration) (*ledger.Connector, error)
	DeleteConnector(ctx context.Context, id string) error
	GetConnector(ctx context.Context, id string) (*ledger.Connector, error)

	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error)
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
	CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error)
	DeletePipeline(ctx context.Context, id string) error
	StartPipeline(ctx context.Context, id string) error
	ResetPipeline(ctx context.Context, id string) error
	StopPipeline(ctx context.Context, id string) error
}

type DefaultController struct {
	driver                     Driver
	listener                   ledgercontroller.Listener
	parser                     ledgercontroller.NumscriptParser
	registry                   *ledgercontroller.StateRegistry
	databaseRetryConfiguration DatabaseRetryConfiguration
	connectorsConfigValidator  ConfigValidator

	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	enableFeatures bool
}

func (ctrl *DefaultController) ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error) {
	return ctrl.driver.GetSystemStore().ListConnectors(ctx)
}

// CreateConnector can return following errors:
// * ErrInvalidDriverConfiguration
func (ctrl *DefaultController) CreateConnector(ctx context.Context, configuration ledger.ConnectorConfiguration) (*ledger.Connector, error) {

	if err := ctrl.connectorsConfigValidator.ValidateConfig(configuration.Driver, configuration.Config); err != nil {
		return nil, NewErrInvalidDriverConfiguration(configuration.Driver, err)
	}

	connector := ledger.NewConnector(configuration)
	if err := ctrl.driver.GetSystemStore().CreateConnector(ctx, connector); err != nil {
		return nil, err
	}
	return &connector, nil
}

// DeleteConnector can return following errors:
// ErrConnectorNotFound
func (ctrl *DefaultController) DeleteConnector(ctx context.Context, id string) error {
	if err := ctrl.driver.GetSystemStore().DeleteConnector(ctx, id); err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return NewErrConnectorNotFound(id)
		default:
			return err
		}
	}
	return nil
}

// GetConnector can return following errors:
// ErrConnectorNotFound
func (ctrl *DefaultController) GetConnector(ctx context.Context, id string) (*ledger.Connector, error) {
	connector, err := ctrl.driver.GetSystemStore().GetConnector(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, NewErrConnectorNotFound(id)
		default:
			return nil, err
		}
	}
	return connector, nil
}

func (ctrl *DefaultController) ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error) {
	return ctrl.driver.GetSystemStore().ListPipelines(ctx)
}

func (ctrl *DefaultController) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	pipeline, err := ctrl.driver.GetSystemStore().GetPipeline(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewErrPipelineNotFound(id)
		}
		return nil, err
	}

	return pipeline, nil
}

func (ctrl *DefaultController) CreatePipeline(ctx context.Context, pipelineConfiguration ledger.PipelineConfiguration) (*ledger.Pipeline, error) {
	pipeline := ledger.NewPipeline(pipelineConfiguration)

	err := ctrl.driver.GetSystemStore().CreatePipeline(ctx, pipeline)
	if err != nil {
		return nil, err
	}

	return &pipeline, nil
}

func (ctrl *DefaultController) DeletePipeline(ctx context.Context, id string) error {
	if err := ctrl.driver.GetSystemStore().DeletePipeline(ctx, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}
	return nil
}

func (ctrl *DefaultController) StartPipeline(ctx context.Context, id string) error {
	if err := ctrl.driver.GetSystemStore().UpdatePipeline(ctx, id, map[string]any{
		"enabled": true,
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}
	return nil
}

func (ctrl *DefaultController) ResetPipeline(ctx context.Context, id string) error {
	if err := ctrl.driver.GetSystemStore().UpdatePipeline(ctx, id, map[string]any{
		"enabled":     true,
		"last_log_id": nil,
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}
	return nil
}

func (ctrl *DefaultController) StopPipeline(ctx context.Context, id string) error {
	if err := ctrl.driver.GetSystemStore().UpdatePipeline(ctx, id, map[string]any{
		"enabled": false,
	}); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ledger.NewErrPipelineNotFound(id)
		}
		return err
	}
	return nil
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
			ctrl.parser,
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

func (ctrl *DefaultController) ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[systemstore.ListLedgersQueryPayload]) (*bunpaginate.Cursor[ledger.Ledger], error) {
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

func NewDefaultController(store Driver, listener ledgercontroller.Listener, validator ConfigValidator, opts ...Option) *DefaultController {
	ret := &DefaultController{
		connectorsConfigValidator: validator,
		driver:                    store,
		listener:                  listener,
		registry:                  ledgercontroller.NewStateRegistry(),
		parser:                    ledgercontroller.NewDefaultNumscriptParser(),
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type Option func(ctrl *DefaultController)

func WithParser(parser ledgercontroller.NumscriptParser) Option {
	return func(ctrl *DefaultController) {
		ctrl.parser = parser
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
