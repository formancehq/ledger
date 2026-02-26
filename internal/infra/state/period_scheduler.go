package state

import (
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
)

// PeriodScheduler runs on every node but only triggers period rotation on the leader.
// It watches the Machine's period schedule cron expression and proposes ClosePeriod
// orders when the cron fires.
type PeriodScheduler struct {
	logger            logging.Logger
	isLeader          func() bool
	getPeriodSchedule func() string
	proposeFn         func()
	scheduleChanged   signal.Signal
	stopCh            chan struct{}
	doneCh            chan struct{}
}

// NewPeriodScheduler creates a new PeriodScheduler.
// proposeFn should propose a ClosePeriod via the admission layer.
func NewPeriodScheduler(
	logger logging.Logger,
	isLeader func() bool,
	getPeriodSchedule func() string,
	proposeFn func(),
	scheduleChanged signal.Signal,
) *PeriodScheduler {
	return &PeriodScheduler{
		logger:            logger,
		isLeader:          isLeader,
		getPeriodSchedule: getPeriodSchedule,
		proposeFn:         proposeFn,
		scheduleChanged:   scheduleChanged,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}
}

// Start launches the background scheduler goroutine.
func (ps *PeriodScheduler) Start() {
	go ps.run()
}

// Stop signals the scheduler to stop and waits for it to finish.
func (ps *PeriodScheduler) Stop() {
	close(ps.stopCh)
	<-ps.doneCh
}

// run is the main scheduler loop.
func (ps *PeriodScheduler) run() {
	defer close(ps.doneCh)

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	resetTimer := func() <-chan time.Time {
		if timer != nil {
			timer.Stop()
		}

		cronExpr := ps.getPeriodSchedule()
		if cronExpr == "" {
			timer = nil
			return nil
		}

		schedule, err := processing.CronParser.Parse(cronExpr)
		if err != nil {
			ps.logger.WithFields(map[string]any{
				"cron":  cronExpr,
				"error": err,
			}).Errorf("Invalid period schedule cron expression, disabling scheduler")
			timer = nil
			return nil
		}

		nextFire := schedule.Next(time.Now())
		delay := time.Until(nextFire)
		if delay < 0 {
			delay = 0
		}

		ps.logger.WithFields(map[string]any{
			"cron":     cronExpr,
			"nextFire": nextFire.Format(time.RFC3339),
		}).Infof("Period scheduler armed")

		timer = time.NewTimer(delay)
		return timer.C
	}

	timerCh := resetTimer()

	for {
		select {
		case <-ps.stopCh:
			return
		case <-ps.scheduleChanged.C():
			timerCh = resetTimer()
		case <-timerCh:
			if ps.isLeader() {
				ps.logger.Infof("Period scheduler firing: proposing ClosePeriod")
				ps.proposeFn()
			}
			timerCh = resetTimer()
		}
	}
}
