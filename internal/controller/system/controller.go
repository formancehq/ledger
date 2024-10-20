package system

import (
	"context"
	"reflect"
	"time"

	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

type Controller interface {
	GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error)
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
	// CreateLedger can return following errors:
	//  * ErrLedgerAlreadyExists
	//  * ledger.ErrInvalidLedgerName
	// It create the ledger in system store and the underlying storage
	CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error
	UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
}

type DefaultController struct {
	store                      Store
	listener                   ledgercontroller.Listener
	parser                     ledgercontroller.NumscriptParser
	registry                   *ledgercontroller.StateRegistry
	databaseRetryConfiguration DatabaseRetryConfiguration

	tracer         trace.Tracer
	meter          metric.Meter
	enableFeatures bool
}

func (ctrl *DefaultController) GetLedgerController(ctx context.Context, name string) (ledgercontroller.Controller, error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetLedgerController", func(ctx context.Context) (ledgercontroller.Controller, error) {
		store, l, err := ctrl.store.OpenLedger(ctx, name)
		if err != nil {
			return nil, err
		}

		var ledgerController ledgercontroller.Controller = ledgercontroller.NewDefaultController(
			*l,
			store,
			ctrl.parser,
			ledgercontroller.WithMeter(ctrl.meter),
		)

		// Add too many client error handling
		ledgerController = ledgercontroller.NewControllerWithTooManyClientHandling(
			ledgerController,
			ctrl.tracer,
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
		ledgerController = ledgercontroller.NewControllerWithTraces(ledgerController, ctrl.tracer)

		// Add events listener
		if ctrl.listener != nil {
			ledgerController = ledgercontroller.NewControllerWithEvents(*l, ledgerController, ctrl.listener)
		}

		return ledgerController, nil
	})
}

func (ctrl *DefaultController) CreateLedger(ctx context.Context, name string, configuration ledger.Configuration) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracer, "CreateLedger", tracing.NoResult(func(ctx context.Context) error {
		configuration.SetDefaults()

		if !ctrl.enableFeatures {
			if !reflect.DeepEqual(configuration.Features, ledger.DefaultFeatures) {
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
	return tracing.Trace(ctx, ctrl.tracer, "GetLedger", func(ctx context.Context) (*ledger.Ledger, error) {
		return ctrl.store.GetLedger(ctx, name)
	})
}

func (ctrl *DefaultController) ListLedgers(ctx context.Context, query ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return tracing.Trace(ctx, ctrl.tracer, "ListLedgers", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Ledger], error) {
		return ctrl.store.ListLedgers(ctx, query)
	})
}

func (ctrl *DefaultController) UpdateLedgerMetadata(ctx context.Context, name string, m map[string]string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracer, "UpdateLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.UpdateLedgerMetadata(ctx, name, m)
	})))
}

func (ctrl *DefaultController) DeleteLedgerMetadata(ctx context.Context, param string, key string) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracer, "DeleteLedgerMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.store.DeleteLedgerMetadata(ctx, param, key)
	})))
}

func NewDefaultController(store Store, listener ledgercontroller.Listener, opts ...Option) *DefaultController {
	ret := &DefaultController{
		store:    store,
		listener: listener,
		registry: ledgercontroller.NewStateRegistry(),
		parser:   ledgercontroller.NewDefaultNumscriptParser(),
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type Option func(ctrl *DefaultController)

func WithUpdateParser(updateParser func(oldParser ledgercontroller.NumscriptParser) ledgercontroller.NumscriptParser) Option {
	return func(ctrl *DefaultController) {
		ctrl.parser = updateParser(ctrl.parser)
	}
}

func WithDatabaseRetryConfiguration(configuration DatabaseRetryConfiguration) Option {
	return func(ctrl *DefaultController) {
		ctrl.databaseRetryConfiguration = configuration
	}
}

func WithMeter(m metric.Meter) Option {
	return func(ctrl *DefaultController) {
		ctrl.meter = m
	}
}

func WithTracer(t trace.Tracer) Option {
	return func(ctrl *DefaultController) {
		ctrl.tracer = t
	}
}

func WithEnableFeatures(v bool) Option {
	return func(ctrl *DefaultController) {
		ctrl.enableFeatures = v
	}
}

var defaultOptions = []Option{
	WithUpdateParser(func(oldParser ledgercontroller.NumscriptParser) ledgercontroller.NumscriptParser {
		return ledgercontroller.NewDefaultNumscriptParser()
	}),
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
}
