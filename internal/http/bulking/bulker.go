package bulking

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/alitto/pond"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

var ErrAtomicParallelConflict = errors.New("atomic and parallel options are mutually exclusive")

type Bulker struct {
	ledger      service.Controller
	ledgerName  string
	parallelism int
	tracer      trace.Tracer
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

func (b *Bulker) processElement(ctx context.Context, data BulkElement) (any, uint64, error) {
	switch data.Action {
	case ActionCreateTransaction:
		req := data.Data.(TransactionRequest)
		rs, err := req.ToCore()
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing element: %s", err)
		}

		log, err := b.ledger.CreateTransaction(ctx, b.ledgerName, service.Parameters[*ledgerpb.CreateTransactionRequestPayload]{
			IdempotencyKey: data.IdempotencyKey,
			Input:          rs,
		})
		if err != nil {
			return nil, 0, err
		}

		return log.Data.Payload.(*ledgerpb.LogPayload_CreatedTransaction).CreatedTransaction.Transaction, log.Id, nil

	case ActionAddMetadata:
		req := data.Data.(AddMetadataRequest)
		var log *ledgerpb.Log
		var err error

		switch req.TargetType {
		case "ACCOUNT":
			address := *req.TargetID.Str

			log, err = b.ledger.SaveAccountMetadata(ctx, b.ledgerName, service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]{
				IdempotencyKey: data.IdempotencyKey,
				Input: &ledgerpb.SaveAccountMetadataRequestPayload{
					Address:  address,
					Metadata: &ledgerpb.Metadata{Entries: req.Metadata},
				},
			})
		case "TRANSACTION":
			transactionID := *req.TargetID.Int

			log, err = b.ledger.SaveTransactionMetadata(ctx, b.ledgerName, service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]{
				IdempotencyKey: data.IdempotencyKey,
				Input: &ledgerpb.SaveTransactionMetadataRequestPayload{
					TransactionId: transactionID,
					Metadata:      &ledgerpb.Metadata{Entries: req.Metadata},
				},
			})
		default:
			return nil, 0, fmt.Errorf("unsupported target type: %s", req.TargetType)
		}
		if err != nil {
			return nil, 0, err
		}

		return nil, log.Id, nil

	case ActionRevertTransaction:
		req := data.Data.(RevertTransactionRequest)

		log, err := b.ledger.RevertTransaction(ctx, b.ledgerName, service.Parameters[*ledgerpb.RevertTransactionRequestPayload]{
			IdempotencyKey: data.IdempotencyKey,
			Input: &ledgerpb.RevertTransactionRequestPayload{
				TransactionId:   req.ID,
				Force:           req.Force,
				AtEffectiveDate: req.AtEffectiveDate,
				Metadata:        req.Metadata,
			},
		})
		if err != nil {
			return nil, 0, err
		}

		return nil, log.Id, nil

	case ActionDeleteMetadata:
		req := data.Data.(DeleteMetadataRequest)
		var (
			log *ledgerpb.Log
		)

		switch req.TargetType {
		case "ACCOUNT":
			address := ""
			targetIDBytes, err := json.Marshal(req.TargetID)
			if err != nil {
				return nil, 0, err
			}
			if err := json.Unmarshal(targetIDBytes, &address); err != nil {
				return nil, 0, err
			}

			log, err = b.ledger.DeleteAccountMetadata(ctx, b.ledgerName, service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]{
				IdempotencyKey: data.IdempotencyKey,
				Input: &ledgerpb.DeleteAccountMetadataRequestPayload{
					Address: address,
					Key:     req.Key,
				},
			})
			if err != nil {
				return nil, 0, err
			}
		case "TRANSACTION":
			transactionID := uint64(0)
			targetIDBytes, err := json.Marshal(req.TargetID)
			if err != nil {
				return nil, 0, err
			}
			if err := json.Unmarshal(targetIDBytes, &transactionID); err != nil {
				return nil, 0, err
			}

			log, err = b.ledger.DeleteTransactionMetadata(ctx, b.ledgerName, service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]{
				IdempotencyKey: data.IdempotencyKey,
				Input: &ledgerpb.DeleteTransactionMetadataRequestPayload{
					TransactionId: transactionID,
					Key:           req.Key,
				},
			})
			if err != nil {
				return nil, 0, err
			}
		default:
			return nil, 0, fmt.Errorf("unsupported target type: %s", req.TargetType)
		}

		return nil, log.Id, nil

	default:
		return nil, 0, fmt.Errorf("unsupported action: %s", data.Action)
	}
}

func NewBulker(ledgerCluster service.Controller, ledgerName string, options ...BulkerOption) *Bulker {
	ret := &Bulker{
		ledger:     ledgerCluster,
		ledgerName: ledgerName,
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
	CreateBulker(ctrl service.Controller, ledgerName string) *Bulker
}

type DefaultBulkerFactory struct {
	Options []BulkerOption
}

func (d *DefaultBulkerFactory) CreateBulker(ledgerCluster service.Controller, ledgerName string) *Bulker {
	return NewBulker(ledgerCluster, ledgerName, d.Options...)
}

func NewDefaultBulkerFactory(options ...BulkerOption) *DefaultBulkerFactory {
	return &DefaultBulkerFactory{
		Options: options,
	}
}

var _ BulkerFactory = (*DefaultBulkerFactory)(nil)
