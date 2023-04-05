package worker

import (
	"context"
)

type Job[MODEL any] func(context.Context, []MODEL) error

type modelsHolder[MODEL any] struct {
	models  []MODEL
	errChan chan error
}

type Worker[MODEL any] struct {
	pending      []modelsHolder[MODEL]
	writeChannel chan modelsHolder[MODEL]
	jobs         chan []modelsHolder[MODEL]
	releasedJob  chan struct{}
	workerJob    Job[MODEL]
	stopChan     chan chan struct{}
}

type WorkerConfig struct {
	MaxPendingSize   int
	MaxWriteChanSize int
}

var (
	DefaultConfig = WorkerConfig{
		MaxPendingSize:   0,
		MaxWriteChanSize: 1024,
	}
)

func NewWorker[MODEL any](workerJob Job[MODEL], cfg WorkerConfig) *Worker[MODEL] {
	return &Worker[MODEL]{
		workerJob:    workerJob,
		pending:      make([]modelsHolder[MODEL], cfg.MaxPendingSize),
		jobs:         make(chan []modelsHolder[MODEL]),
		writeChannel: make(chan modelsHolder[MODEL], cfg.MaxWriteChanSize),
		releasedJob:  make(chan struct{}, 1),
		stopChan:     make(chan chan struct{}, 1),
	}
}

func (w *Worker[MODEL]) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case w.releasedJob <- struct{}{}:
		case modelsHolders := <-w.jobs:
			models := make([]MODEL, 0)
			for _, holder := range modelsHolders {
				models = append(models, holder.models...)
			}
			err := w.workerJob(ctx, models)
			go func() {
				for _, holder := range modelsHolders {
					select {
					case <-ctx.Done():
						return
					case holder.errChan <- err:
						close(holder.errChan)
					}
				}
			}()
		}
	}
}

// Run should be called in a goroutine
func (w *Worker[MODEL]) Run(ctx context.Context) {

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
						w.pending = make([]modelsHolder[MODEL], 0)
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
				case w.jobs <- []modelsHolder[MODEL]{mh}:
				}
			}
		}
	}
}

func (w *Worker[MODEL]) Stop(ctx context.Context) error {
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

func (w *Worker[MODEL]) WriteModels(ctx context.Context, models ...MODEL) <-chan error {
	errChan := make(chan error, 1)

	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		close(errChan)
	case w.writeChannel <- modelsHolder[MODEL]{
		models:  models,
		errChan: errChan,
	}:
	}

	return errChan
}
