package replication

import (
	"context"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/storage/common"
)

var (
	DefaultPullInterval    = 10 * time.Second
	DefaultPushRetryPeriod = 10 * time.Second
)

type PipelineHandlerConfig struct {
	PullInterval    time.Duration
	PushRetryPeriod time.Duration
	LogsPageSize    uint64
}

type PipelineOption func(config *PipelineHandlerConfig)

func WithPullPeriod(v time.Duration) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.PullInterval = v
	}
}

func WithPushRetryPeriod(v time.Duration) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.PushRetryPeriod = v
	}
}

func WithLogsPageSize(v uint64) PipelineOption {
	return func(config *PipelineHandlerConfig) {
		config.LogsPageSize = v
	}
}

var (
	defaultPipelineOptions = []PipelineOption{
		WithPullPeriod(DefaultPullInterval),
		WithPushRetryPeriod(DefaultPushRetryPeriod),
		WithLogsPageSize(100),
	}
)

type PipelineHandler struct {
	pipeline       ledger.Pipeline
	stopChannel    chan chan error
	store          LogFetcher
	exporter       drivers.Driver
	pipelineConfig PipelineHandlerConfig
	logger         logging.Logger
	rewriter       *AddressRewriter
	// rewriterErr holds a failure to compile the pipeline's address rewrite rules.
	// Rules are validated at creation time, so this should never occur in practice;
	// if it does we fail loudly rather than export unrewritten data.
	rewriterErr error
}

func (p *PipelineHandler) Run(ctx context.Context, ingestedLogs chan uint64) {
	p.logger.Debugf("Pipeline started.")
	nextInterval := time.Duration(0)

	stop := func(ch chan error) {
		p.logger.Debugf("Pipeline terminated.")
		close(ch)
	}

	for {
		select {
		case ch := <-p.stopChannel:
			stop(ch)
			return
		case <-time.After(nextInterval):
			p.logger.Debugf("Fetch next batch.")
			var builder query.Builder
			if p.pipeline.LastLogID != nil {
				builder = query.Gt("id", *p.pipeline.LastLogID)
			}
			logs, err := p.store.ListLogs(ctx, common.InitialPaginatedQuery[any]{
				PageSize: p.pipelineConfig.LogsPageSize,
				Column:   "id",
				Options: common.ResourceQuery[any]{
					Builder: builder,
				},
				Order: pointer.For(paginate.Order(paginate.OrderAsc)),
			})
			if err != nil {
				p.logger.Errorf("Error fetching logs: %s", err)
				select {
				case ch := <-p.stopChannel:
					stop(ch)
					return
				case <-time.After(p.pipelineConfig.PullInterval):
					continue
				}
			}

			p.logger.Debugf("Got %d items", len(logs.Data))
			if len(logs.Data) == 0 {
				nextInterval = p.pipelineConfig.PullInterval
				continue
			}

			if p.rewriterErr != nil {
				p.logger.Errorf("Invalid address rewrite configuration: %s, waiting for: %s", p.rewriterErr, p.pipelineConfig.PushRetryPeriod)
				select {
				case ch := <-p.stopChannel:
					stop(ch)
					return
				case <-time.After(p.pipelineConfig.PushRetryPeriod):
					continue
				}
			}

			logsToExport := make([]drivers.LogWithLedger, 0, len(logs.Data))
			var rewriteErr error
			for _, log := range logs.Data {
				rewritten, err := p.rewriter.Apply(log)
				if err != nil {
					rewriteErr = err
					break
				}
				logsToExport = append(logsToExport, drivers.LogWithLedger{
					Log:    rewritten,
					Ledger: p.pipeline.Ledger,
				})
			}
			if rewriteErr != nil {
				p.logger.Errorf("Error rewriting addresses: %s, waiting for: %s", rewriteErr, p.pipelineConfig.PushRetryPeriod)
				select {
				case ch := <-p.stopChannel:
					stop(ch)
					return
				case <-time.After(p.pipelineConfig.PushRetryPeriod):
					continue
				}
			}

			for {
				p.logger.Debugf("Send data to exporter.")
				errChan := make(chan error, 1)
				exportContext, cancel := context.WithCancel(ctx)
				go func() {
					_, err := p.exporter.Accept(exportContext, logsToExport...)
					errChan <- err
				}()
				select {
				case err := <-errChan:
					cancel()
					if err != nil {
						p.logger.Errorf("Error pushing data on exporter: %s, waiting for: %s", err, p.pipelineConfig.PushRetryPeriod)
						select {
						case ch := <-p.stopChannel:
							stop(ch)
							return
						case <-time.After(p.pipelineConfig.PushRetryPeriod):
							continue
						}
					}
				case ch := <-p.stopChannel:
					cancel()
					stop(ch)
					return
				}

				break
			}

			lastLogID := logs.Data[len(logs.Data)-1].ID
			p.logger.Debugf("Move last log id to %d", *lastLogID)
			p.pipeline.LastLogID = lastLogID

			select {
			case <-ctx.Done():
				return
			case ingestedLogs <- *lastLogID:
			}

			if !logs.HasMore {
				nextInterval = p.pipelineConfig.PullInterval
			} else {
				p.logger.Debugf("Has more logs to fetch.")
				nextInterval = 0
			}
		}
	}
}

func (p *PipelineHandler) Shutdown(ctx context.Context) error {
	p.logger.Infof("Shutting down pipeline")
	errorChannel := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.stopChannel <- errorChannel:
		p.logger.Debugf("shutdowning pipeline signal sent")
		select {
		case err := <-errorChannel:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func NewPipelineHandler(
	pipeline ledger.Pipeline,
	store LogFetcher,
	driver drivers.Driver,
	logger logging.Logger,
	opts ...PipelineOption,
) *PipelineHandler {
	config := PipelineHandlerConfig{}
	for _, opt := range append(defaultPipelineOptions, opts...) {
		opt(&config)
	}

	rewriter, rewriterErr := NewAddressRewriter(pipeline.AddressRewriteRules)

	return &PipelineHandler{
		pipeline:       pipeline,
		stopChannel:    make(chan chan error, 1),
		store:          store,
		exporter:       driver,
		pipelineConfig: config,
		rewriter:       rewriter,
		rewriterErr:    rewriterErr,
		logger: logger.
			WithField("component", "pipeline").
			WithField("module", pipeline.Ledger).
			WithField("driver", pipeline.ExporterID),
	}
}
