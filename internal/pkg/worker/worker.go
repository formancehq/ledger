package worker

// Worker provides goroutine lifecycle management (start/stop) for background
// workers. It encapsulates the stopCh/doneCh pattern shared across all workers.
type Worker struct {
	stopCh chan struct{}
	doneCh chan struct{}
}

// New creates a new Worker with initialized channels.
func New() Worker {
	return Worker{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Run launches fn in a background goroutine. The goroutine signals completion
// by closing doneCh when fn returns. fn receives stopCh to monitor for shutdown.
func (w *Worker) Run(fn func(stop <-chan struct{})) {
	go func() {
		defer close(w.doneCh)
		fn(w.stopCh)
	}()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (w *Worker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

// StopCh returns the stop channel for use in select statements within callbacks.
func (w *Worker) StopCh() <-chan struct{} {
	return w.stopCh
}
