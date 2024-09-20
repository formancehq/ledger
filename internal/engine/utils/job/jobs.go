package job

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/logging"
	"github.com/pkg/errors"
)

type Job interface {
	Terminated()
}

type builtJob struct {
	terminatedFn func()
}

func (j builtJob) Terminated() {
	j.terminatedFn()
}

func newJob(terminatedFn func()) *builtJob {
	return &builtJob{
		terminatedFn: terminatedFn,
	}
}

type Runner[JOB Job] struct {
	stopChan         chan chan struct{}
	runner           func(context.Context, *JOB) error
	nbWorkers        int
	parkedWorkers    atomic.Int64
	nextJob          func() *JOB
	jobs             chan *JOB
	newJobsAvailable chan struct{}
}

func (r *Runner[JOB]) Next() {
	r.newJobsAvailable <- struct{}{}
}

func (r *Runner[JOB]) Close() {
	done := make(chan struct{})
	r.stopChan <- done
	<-done
}

func (r *Runner[JOB]) Run(ctx context.Context) {

	logger := logging.FromContext(ctx)
	logger.Infof("Start worker")

	defer func() {
		if e := recover(); e != nil {
			logger.Error(e)
			debug.PrintStack()
			panic(e)
		}
	}()

	terminatedJobs := make(chan *JOB, r.nbWorkers)
	jobsErrors := make(chan error, r.nbWorkers)

	w := pond.New(r.nbWorkers, r.nbWorkers)
	for i := 0; i < r.nbWorkers; i++ {
		i := i
		w.Submit(func() {
			defer func() {
				if e := recover(); e != nil {
					if err, isError := e.(error); isError {
						jobsErrors <- errors.WithStack(err)
						return
					}
					jobsErrors <- errors.WithStack(fmt.Errorf("%s", e))
				}
			}()
			logger := logger.WithFields(map[string]any{
				"worker": i,
			})
			for {
				select {
				case job, ok := <-r.jobs:
					if !ok {
						logger.Debugf("Worker %d stopped", i)
						return
					}
					logger := logger.WithField("job", job)
					logger.Debugf("Got new job")
					if err := r.runner(ctx, job); err != nil {
						panic(err)
					}
					logger.Debugf("Job terminated")
					terminatedJobs <- job
				}
			}
		})
	}

	for {
		select {
		case jobError := <-jobsErrors:
			panic(jobError)
		case done := <-r.stopChan:
			close(r.jobs)
			w.StopAndWait()
			close(terminatedJobs)
			close(done)
			return
		case <-r.newJobsAvailable:
			if r.parkedWorkers.Load() > 0 {
				if job := r.nextJob(); job != nil {
					r.jobs <- job
					r.parkedWorkers.Add(-1)
				}
			}
		case job := <-terminatedJobs:
			(*job).Terminated()
			if job := r.nextJob(); job != nil {
				r.jobs <- job
			} else {
				r.parkedWorkers.Add(1)
			}
		}
	}
}

func NewJobRunner[JOB Job](runner func(context.Context, *JOB) error, nextJob func() *JOB, nbWorkers int) *Runner[JOB] {
	parkedWorkers := atomic.Int64{}
	parkedWorkers.Add(int64(nbWorkers))
	return &Runner[JOB]{
		stopChan:         make(chan chan struct{}),
		runner:           runner,
		nbWorkers:        nbWorkers,
		parkedWorkers:    parkedWorkers,
		nextJob:          nextJob,
		jobs:             make(chan *JOB, nbWorkers),
		newJobsAvailable: make(chan struct{}),
	}
}
