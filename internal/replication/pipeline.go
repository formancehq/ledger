package replication

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

var (
	DefaultPullInterval    = 10 * time.Second
	DefaultPushRetryPeriod = 10 * time.Second
)

type PipelineHandlerConfig struct {
	PullInterval    time.Duration
	PushRetryPeriod time.Duration
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

var (
	defaultPipelineOptions = []PipelineOption{
		WithPullPeriod(DefaultPullInterval),
		WithPushRetryPeriod(DefaultPushRetryPeriod),
	}
)

type PipelineHandler struct {
	pipeline       ledger.Pipeline
	stopChannel    chan chan error
	store          LogFetcher
	connector      drivers.Driver
	pipelineConfig PipelineHandlerConfig
	logger         logging.Logger
}

func (p *PipelineHandler) Run(ctx context.Context, ingestedLogs chan uint64) {
	nextInterval := time.Duration(0)
	for {
		select {
		case ch := <-p.stopChannel:
			close(ch)
			return
		case <-time.After(nextInterval):
			logs, err := p.store.ListLogs(ctx, common.InitialPaginatedQuery[any]{
				// todo: make configurable
				PageSize: 100,
				Column:   "id",
				Options: common.ResourceQuery[any]{
					Builder: query.Gt("id", p.pipeline.LastLogID),
				},
				Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
			})
			if err != nil {
				p.logger.Errorf("Error fetching logs: %s", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(p.pipelineConfig.PullInterval):
					continue
				}
			}

			if len(logs.Data) == 0 {
				nextInterval = p.pipelineConfig.PullInterval
				continue
			}

			for {
				_, err := p.connector.Accept(ctx, collectionutils.Map(logs.Data, func(log ledger.Log) drivers.LogWithLedger {
					return drivers.LogWithLedger{
						Log:    log,
						Ledger: p.pipeline.Ledger,
					}
				})...)
				if err != nil {
					p.logger.Errorf("Error pushing data on connector: %s, waiting for: %s", err, p.pipelineConfig.PushRetryPeriod)
					select {
					case <-ctx.Done():
						return
					case <-time.After(p.pipelineConfig.PushRetryPeriod):
						continue
					}
				}
				break
			}

			lastLogID := logs.Data[len(logs.Data)-1].ID
			p.pipeline.LastLogID = lastLogID

			select {
			case <-ctx.Done():
				return
			case ingestedLogs <- *lastLogID:
			}

			if !logs.HasMore {
				nextInterval = p.pipelineConfig.PullInterval
			} else {
				nextInterval = 0
			}
		}
	}
}

func (p *PipelineHandler) Shutdown(ctx context.Context) error {
	p.logger.Infof("shutdowning pipeline")
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
	connector drivers.Driver,
	logger logging.Logger,
	opts ...PipelineOption,
) *PipelineHandler {
	config := PipelineHandlerConfig{}
	for _, opt := range append(defaultPipelineOptions, opts...) {
		opt(&config)
	}

	return &PipelineHandler{
		pipeline:       pipeline,
		stopChannel:    make(chan chan error, 1),
		store:          store,
		connector:      connector,
		pipelineConfig: config,
		logger: logger.
			WithField("component", "pipeline").
			WithField("module", pipeline.Ledger).
			WithField("connector", pipeline.ConnectorID),
	}
}
