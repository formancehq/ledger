package bulking

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/alitto/pond"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

var ErrAtomicParallelConflict = errors.New("atomic and parallel options are mutually exclusive")

type Bulker struct {
	ledger      service.Controller
	ledgerID    uint32
	parallelism int
	tracer      trace.Tracer
}

func (b *Bulker) run(ctx context.Context, bulk Bulk, result chan *LedgerActionResult, continueOnFailure, parallel bool) bool {

	submit := func(fn func()) {
		fn()
	}
	wait := func() {}
	hasError := atomic.Bool{}

	parallelism := 1
	if parallel && b.parallelism != 0 {
		parallelism = b.parallelism
	}
	if parallelism > 1 {
		wp := pond.New(parallelism, parallelism)
		submit = wp.Submit
		wait = wp.StopAndWait
	}

	index := 0
	for element := range bulk {
		// Copy to prevent data race
		itemIndex := index
		submit(func() {
			ctx, span := b.tracer.Start(ctx, "Bulk:ProcessElement",
				trace.WithNewRoot(),
				trace.WithLinks(trace.LinkFromContext(ctx)),
				trace.WithAttributes(attribute.Int("index", itemIndex)),
			)
			defer span.End()

			select {
			case <-ctx.Done():
				result <- NewLedgerActionResult(itemIndex, nil, ctx.Err())
			default:
				if hasError.Load() && !continueOnFailure {
					result <- NewLedgerActionResult(itemIndex, nil, context.Canceled)
					return
				}
				log, err := b.processElement(ctx, element)
				if err != nil {
					hasError.Store(true)
					otlp.RecordError(ctx, err)
					result <- NewLedgerActionResult(itemIndex, nil, err)
					return
				}

				result <- NewLedgerActionResult(itemIndex, log, nil)
			}

		})
		index++
	}

	wait()

	defer close(result)

	return hasError.Load()
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, result chan *LedgerActionResult, bulkOptions BulkingOptions) error {
	ctx, span := b.tracer.Start(ctx, "Bulk:Run", trace.WithAttributes(
		attribute.Bool("atomic", bulkOptions.Atomic),
		attribute.Bool("parallel", bulkOptions.Parallel),
		attribute.Bool("continueOnFailure", bulkOptions.ContinueOnFailure),
		attribute.Int("parallelism", b.parallelism),
	))
	defer span.End()

	if err := bulkOptions.Validate(); err != nil {
		return fmt.Errorf("validating bulk options: %w", err)
	}

	// Note: Atomic transactions are not yet supported in this implementation
	if bulkOptions.Atomic {
		return fmt.Errorf("atomic bulk transactions are not yet supported")
	}

	hasError := b.run(ctx, bulk, result, bulkOptions.ContinueOnFailure, bulkOptions.Parallel)
	if hasError && bulkOptions.Atomic {
		// Would rollback here if atomic transactions were supported
		return nil
	}

	return nil
}

func (b *Bulker) processElement(ctx context.Context, elem *servicepb.LedgerAction) (*commonpb.LedgerLog, error) {
	// Set the ledger ID on the action before applying
	elem.LedgerId = b.ledgerID
	return b.ledger.Apply(ctx, elem)
}

func NewBulker(ledgerCluster service.Controller, ledgerID uint32, options ...BulkerOption) *Bulker {
	ret := &Bulker{
		ledger:   ledgerCluster,
		ledgerID: ledgerID,
	}
	for _, option := range append(defaultBulkerOptions, options...) {
		option(ret)
	}

	return ret
}

type BulkerOption func(bulker *Bulker)

func WithParallelism(v int) BulkerOption {
	return func(options *Bulker) {
		options.parallelism = v
	}
}

func WithTracer(tracer trace.Tracer) BulkerOption {
	return func(options *Bulker) {
		options.tracer = tracer
	}
}

var defaultBulkerOptions = []BulkerOption{
	WithTracer(noop.Tracer{}),
	WithParallelism(10),
}

type BulkingOptions struct {
	ContinueOnFailure bool
	Atomic            bool
	Parallel          bool
}

func (opts BulkingOptions) Validate() error {
	if opts.Atomic && opts.Parallel {
		return ErrAtomicParallelConflict
	}

	return nil
}

type BulkerFactory interface {
	CreateBulker(ctrl service.Controller, ledgerID uint32) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ledgerCluster service.Controller, ledgerID uint32) *Bulker {
	return NewBulker(ledgerCluster, ledgerID, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)
