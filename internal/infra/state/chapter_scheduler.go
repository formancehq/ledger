package state

import (
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// ChapterScheduler runs on every node but only triggers chapter rotation on the leader.
// It watches the Machine's chapter schedule cron expression and proposes CloseChapter
// orders when the cron fires.
type ChapterScheduler struct {
	logger             logging.Logger
	isLeader           func() bool
	getChapterSchedule func() string
	proposeFn          func() error
	scheduleChanged    signal.Signal
	w                  worker.Worker
}

// NewChapterScheduler creates a new ChapterScheduler.
// proposeFn should propose a CloseChapter via the admission layer.
func NewChapterScheduler(
	logger logging.Logger,
	isLeader func() bool,
	getChapterSchedule func() string,
	proposeFn func() error,
	scheduleChanged signal.Signal,
) *ChapterScheduler {
	return &ChapterScheduler{
		logger:             logger,
		isLeader:           isLeader,
		getChapterSchedule: getChapterSchedule,
		proposeFn:          proposeFn,
		scheduleChanged:    scheduleChanged,
		w:                  worker.New(),
	}
}

// Start launches the background scheduler goroutine.
func (ps *ChapterScheduler) Start() {
	ps.w.Run(ps.loop)
}

// Stop signals the scheduler to stop and waits for it to finish.
func (ps *ChapterScheduler) Stop() {
	ps.w.Stop()
}

// loop is the main scheduler loop.
func (ps *ChapterScheduler) loop(stop <-chan struct{}) {
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

		cronExpr := ps.getChapterSchedule()
		if cronExpr == "" {
			timer = nil

			return nil
		}

		schedule, err := processing.CronParser.Parse(cronExpr)
		if err != nil {
			ps.logger.WithFields(map[string]any{
				"cron":  cronExpr,
				"error": err,
			}).Errorf("Invalid chapter schedule cron expression, disabling scheduler")

			timer = nil

			return nil
		}

		nextFire := schedule.Next(time.Now())

		delay := max(time.Until(nextFire), 0)

		ps.logger.WithFields(map[string]any{
			"cron":     cronExpr,
			"nextFire": nextFire.Format(time.RFC3339),
		}).Infof("Chapter scheduler armed")

		timer = time.NewTimer(delay)

		return timer.C
	}

	timerCh := resetTimer()

	for {
		select {
		case <-stop:
			return
		case <-ps.scheduleChanged.C():
			timerCh = resetTimer()
		case <-timerCh:
			if ps.isLeader() {
				ps.logger.Infof("Chapter scheduler firing: proposing CloseChapter")

				if err := ps.proposeFn(); err != nil {
					ps.logger.Errorf("Failed to propose CloseChapter: %v (will retry on next tick)", err)
				}
			}

			timerCh = resetTimer()
		}
	}
}
