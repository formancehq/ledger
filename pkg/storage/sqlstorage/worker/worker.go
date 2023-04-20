package worker

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
)

type Job func(context.Context, []*core.Log) ([]*core.PersistedLog, error)

type pendingLog struct {
	log       *core.Log
	errChan   chan error
	persisted chan *core.PersistedLog
}

type Worker struct {
	pending      []pendingLog
	writeChannel chan pendingLog
	jobs         chan []pendingLog
	releasedJob  chan struct{}
	workerJob    Job
	stopChan     chan chan struct{}
}

type Config struct {
	MaxPendingSize   int
	MaxWriteChanSize int
}

var (
	DefaultConfig = Config{
		MaxPendingSize:   0,
		MaxWriteChanSize: 1024,
	}
)

func NewWorker(workerJob Job, cfg Config) *Worker {
	return &Worker{
		workerJob:    workerJob,
		pending:      make([]pendingLog, cfg.MaxPendingSize),
		jobs:         make(chan []pendingLog),
		writeChannel: make(chan pendingLog, cfg.MaxWriteChanSize),
		releasedJob:  make(chan struct{}, 1),
		stopChan:     make(chan chan struct{}, 1),
	}
}

func (w *Worker) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case w.releasedJob <- struct{}{}:
		case modelsHolders := <-w.jobs:
			models := make([]*core.Log, 0)
			for _, holder := range modelsHolders {
				models = append(models, holder.log)
			}
			persistedLogs, err := w.workerJob(ctx, models)
			go func() {
				for i, holder := range modelsHolders {
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case holder.errChan <- err:
						}
					} else {
						select {
						case <-ctx.Done():
							return
						case holder.persisted <- persistedLogs[i]:
						}
					}
					close(holder.errChan)
					close(holder.persisted)
				}
			}()
		}
	}
}

// Run should be called in a goroutine
func (w *Worker) Run(ctx context.Context) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go w.writeLoop(ctx)

l:
	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-w.stopChan:
			close(ch)
			return
		// At this level, the job is writting some models, just accumulate models in a buffer
		case mh := <-w.writeChannel:
			w.pending = append(w.pending, mh)
		case <-w.releasedJob:
			// There, write model job is not running, and we have pending models
			// So we can try to send pending to the job channel
			if len(w.pending) > 0 {
				for {
					select {
					case <-ctx.Done():
						return
					case ch := <-w.stopChan:
						close(ch)
						return
					case w.jobs <- w.pending:
						// Models has been handled by the job, just clear pending models
						w.pending = make([]pendingLog, 0)
						continue l
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case ch := <-w.stopChan:
				close(ch)
				return
			// There, the job is waiting, and we don't have any pending models to write
			// so, wait for new models to write and send them directly to the job channel
			// We can not return to the main loop as w.releasedJob will be continuously notified by the job routine
			case mh := <-w.writeChannel:
				select {
				case <-ctx.Done():
					return
				case ch := <-w.stopChan:
					close(ch)
					return
				case w.jobs <- []pendingLog{mh}:
				}
			}
		}
	}
}

func (w *Worker) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.stopChan <- ch:
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *Worker) WriteModel(ctx context.Context, model *core.Log) (<-chan *core.PersistedLog, <-chan error) {
	errChan := make(chan error, 1)
	ret := make(chan *core.PersistedLog, 1)

	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		close(errChan)
	case w.writeChannel <- pendingLog{
		log:       model,
		errChan:   errChan,
		persisted: ret,
	}:
	}

	return ret, errChan
}
