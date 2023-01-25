package stripe

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/logging"
)

func NewRunner(
	logger logging.Logger,
	trigger *TimelineTrigger,
	pollingPeriod time.Duration,
) *Runner {
	return &Runner{
		logger: logger.WithFields(map[string]interface{}{
			"component": "runner",
		}),
		trigger:       trigger,
		pollingPeriod: pollingPeriod,
		stopChan:      make(chan chan struct{}),
	}
}

type Runner struct {
	stopChan      chan chan struct{}
	trigger       *TimelineTrigger
	logger        logging.Logger
	pollingPeriod time.Duration
}

func (r *Runner) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case r.stopChan <- ch:
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) Run(ctx context.Context) error {
	r.logger.WithFields(map[string]interface{}{
		"polling-period": r.pollingPeriod,
	}).Info("Starting runner")
	defer r.trigger.Cancel(ctx)

	done := make(chan struct{}, 1)
	fetch := func() {
		defer func() {
			done <- struct{}{}
		}()

		if err := r.trigger.Fetch(ctx); err != nil {
			r.logger.Errorf("Error fetching page: %s", err)
		}
	}

	go fetch()

	var timeChan <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case closeChannel := <-r.stopChan:
			r.trigger.Cancel(ctx)
			close(closeChannel)

			return nil
		case <-done:
			timeChan = time.After(r.pollingPeriod)
		case <-timeChan:
			timeChan = nil

			go fetch()
		}
	}
}
