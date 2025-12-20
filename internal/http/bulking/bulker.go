package bulking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/alitto/pond"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

var ErrAtomicParallelConflict = errors.New("atomic and parallel options are mutually exclusive")

type Bulker struct {
	ledgerCluster   service.LedgerCluster
	ledgerName      string
	parallelism     int
	tracer          trace.Tracer
}

func (b *Bulker) run(ctx context.Context, bulk Bulk, result chan BulkElementResult, continueOnFailure, parallel bool) bool {
	parallelism := 1
	if parallel && b.parallelism != 0 {
		parallelism = b.parallelism
	}

	wp := pond.New(parallelism, parallelism)
	hasError := atomic.Bool{}

	index := 0
	for element := range bulk {
		// Copy to prevent data race
		itemIndex := index
		wp.Submit(func() {
			ctx, span := b.tracer.Start(ctx, "Bulk:ProcessElement",
				trace.WithNewRoot(),
				trace.WithLinks(trace.LinkFromContext(ctx)),
				trace.WithAttributes(attribute.Int("index", itemIndex)),
			)
			defer span.End()

			select {
			case <-ctx.Done():
				result <- BulkElementResult{
					Error:     ctx.Err(),
					ElementID: itemIndex,
				}
			default:
				if hasError.Load() && !continueOnFailure {
					result <- BulkElementResult{
						Error:     context.Canceled,
						ElementID: itemIndex,
					}
					return
				}
				ret, logID, err := b.processElement(ctx, element)
				if err != nil {
					hasError.Store(true)
					otlp.RecordError(ctx, err)

					result <- BulkElementResult{
						Error:     err,
						ElementID: itemIndex,
					}

					return
				}

				result <- BulkElementResult{
					Data:      ret,
					LogID:     logID,
					ElementID: itemIndex,
				}
			}

		})
		index++
	}

	wp.StopAndWait()

	defer close(result)

	return hasError.Load()
}

func (b *Bulker) Run(ctx context.Context, bulk Bulk, result chan BulkElementResult, bulkOptions BulkingOptions) error {
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
	// as we don't have transaction support in the LedgerCluster interface
	if bulkOptions.Atomic {
		return fmt.Errorf("atomic bulk transactions are not yet supported")
	}

	hasError := b.run(ctx, bulk, result, bulkOptions.ContinueOnFailure, bulkOptions.Parallel)
	if hasError && bulkOptions.Atomic {
		// Would rollback here if atomic transactions were supported
		return nil
	}

	if bulkOptions.Atomic {
		// Would commit here if atomic transactions were supported
	}

	return nil
}

func (b *Bulker) processElement(ctx context.Context, data BulkElement) (any, uint64, error) {
	switch data.Action {
	case ActionCreateTransaction:
		req := data.Data.(TransactionRequest)
		rs, err := req.ToCore()
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, createTransactionResult, err := b.ledgerCluster.CreateTransaction(ctx, b.ledgerName, service.Parameters[service.CreateTransaction]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input:          *rs,
		})
		if err != nil {
			return nil, 0, err
		}

		return createTransactionResult.Transaction, *log.ID, nil

	case ActionAddMetadata:
		req := data.Data.(AddMetadataRequest)
		var log *ledger.Log
		var err error

		switch req.TargetType {
		case "ACCOUNT":
			address := ""
			if err := json.Unmarshal(req.TargetID, &address); err != nil {
				return nil, 0, err
			}

			log, err = b.ledgerCluster.SaveAccountMetadata(ctx, b.ledgerName, service.Parameters[service.SaveAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: service.SaveAccountMetadata{
					Address:  address,
					Metadata: req.Metadata,
				},
			})
		case "TRANSACTION":
			transactionID := uint64(0)
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}

			log, err = b.ledgerCluster.SaveTransactionMetadata(ctx, b.ledgerName, service.Parameters[service.SaveTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: service.SaveTransactionMetadata{
					TransactionID: transactionID,
					Metadata:     req.Metadata,
				},
			})
		default:
			return nil, 0, fmt.Errorf("unsupported target type: %s", req.TargetType)
		}
		if err != nil {
			return nil, 0, err
		}

		return nil, *log.ID, nil

	case ActionRevertTransaction:
		req := data.Data.(RevertTransactionRequest)

		log, _, err := b.ledgerCluster.RevertTransaction(ctx, b.ledgerName, service.Parameters[service.RevertTransaction]{
			DryRun:         false,
			IdempotencyKey: data.IdempotencyKey,
			Input: service.RevertTransaction{
				TransactionID:   req.ID,
				Force:           req.Force,
				AtEffectiveDate: req.AtEffectiveDate,
				Metadata:        req.Metadata,
			},
		})
		if err != nil {
			return nil, 0, err
		}

		return nil, *log.ID, nil

	case ActionDeleteMetadata:
		req := data.Data.(DeleteMetadataRequest)
		var log *ledger.Log
		var err error

		switch req.TargetType {
		case "ACCOUNT":
			address := ""
			if err := json.Unmarshal(req.TargetID, &address); err != nil {
				return nil, 0, err
			}

			log, err = b.ledgerCluster.DeleteAccountMetadata(ctx, b.ledgerName, service.Parameters[service.DeleteAccountMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: service.DeleteAccountMetadata{
					Address: address,
					Key:     req.Key,
				},
			})
		case "TRANSACTION":
			transactionID := uint64(0)
			if err := json.Unmarshal(req.TargetID, &transactionID); err != nil {
				return nil, 0, err
			}

			log, err = b.ledgerCluster.DeleteTransactionMetadata(ctx, b.ledgerName, service.Parameters[service.DeleteTransactionMetadata]{
				DryRun:         false,
				IdempotencyKey: data.IdempotencyKey,
				Input: service.DeleteTransactionMetadata{
					TransactionID: transactionID,
					Key:           req.Key,
				},
			})
		default:
			return nil, 0, fmt.Errorf("unsupported target type: %s", req.TargetType)
		}
		if err != nil {
			return nil, 0, err
		}

		return nil, *log.ID, nil

	default:
		return nil, 0, fmt.Errorf("unsupported action: %s", data.Action)
	}
}

func NewBulker(ledgerCluster service.LedgerCluster, ledgerName string, options ...BulkerOption) *Bulker {
	ret := &Bulker{
		ledgerCluster: ledgerCluster,
		ledgerName:    ledgerName,
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
	CreateBulker(ledgerCluster service.LedgerCluster, ledgerName string) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ledgerCluster service.LedgerCluster, ledgerName string) *Bulker {
	return NewBulker(ledgerCluster, ledgerName, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)

