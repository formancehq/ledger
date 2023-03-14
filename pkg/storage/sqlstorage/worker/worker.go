package worker

import (
	"context"
	"time"
)

type WorkerJob[MODEL any] func(context.Context, []MODEL) error

type Model[MODEL any] struct {
	models  []MODEL
	errChan chan error
}

type Worker[MODEL any] struct {
	ctx       context.Context
	batchSize int
	batchTime time.Duration

	models     []Model[MODEL]
	modelsChan chan Model[MODEL]

	workerJob WorkerJob[MODEL]
}

func NewWorker[MODEL any](batchSize int, batchTime time.Duration, workerJob WorkerJob[MODEL]) *Worker[MODEL] {
	return &Worker[MODEL]{
		batchSize: batchSize,
		batchTime: batchTime,
		workerJob: workerJob,

		models:     nil,
		modelsChan: make(chan Model[MODEL], 1024),
	}
}

// Run should be called in a goroutine
func (w *Worker[MODEL]) Run(ctx context.Context) {
	w.ctx = ctx
	ticker := time.NewTicker(w.batchTime)

	writeFn := func(context.Context) {
		if err := w.write(ctx); err != nil {
			// TODO(polo): should we stop the writer in case of an error ?
			// return err
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case ms := <-w.modelsChan:
			w.models = append(w.models, ms)
			if w.len() >= w.batchSize {
				writeFn(ctx)
			}

		case <-ticker.C:
			writeFn(ctx)
		}
	}
}

func (w *Worker[MODEL]) write(ctx context.Context) error {
	if len(w.models) == 0 {
		return nil
	}

	models := make([]MODEL, 0, w.len())
	for _, model := range w.models {
		models = append(models, model.models...)
	}

	err := w.workerJob(ctx, models)

	for _, model := range w.models {
		model.errChan <- err
		close(model.errChan)
	}

	// Even if the worker job failed, we still want to release the memory, the
	// clients will retry.
	// Release the slice in order to release the underlying memory to the
	// garbage collector.
	w.models = nil

	return err
}

func (w *Worker[MODEL]) len() int {
	l := 0
	for _, model := range w.models {
		l += len(model.models)
	}
	return l
}

func (w *Worker[MODEL]) WriteModels(ctx context.Context, models []MODEL) <-chan error {
	errChan := make(chan error, 1)

	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		close(errChan)
	case w.modelsChan <- Model[MODEL]{
		models:  models,
		errChan: errChan,
	}:
	}

	return errChan
}
