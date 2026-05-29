package state

import (
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// QueryCheckpointScheduler runs on every node but only triggers checkpoint creation on the leader.
// It watches the Machine's query checkpoint schedule cron expression and proposes
// CreateQueryCheckpoint orders when the cron fires.
type QueryCheckpointScheduler struct {
	logger          logging.Logger
	isLeader        func() bool
	getSchedule     func() string
	proposeFn       func()
	scheduleChanged signal.Signal
	w               worker.Worker
}

// NewQueryCheckpointScheduler creates a new QueryCheckpointScheduler.
// proposeFn should propose a CreateQueryCheckpoint via the admission layer.
func NewQueryCheckpointScheduler(
	logger logging.Logger,
	isLeader func() bool,
	getSchedule func() string,
	proposeFn func(),
	scheduleChanged signal.Signal,
) *QueryCheckpointScheduler {
	return &QueryCheckpointScheduler{
		logger:          logger,
		isLeader:        isLeader,
		getSchedule:     getSchedule,
		proposeFn:       proposeFn,
		scheduleChanged: scheduleChanged,
		w:               worker.New(),
	}
}

// Start launches the background scheduler goroutine.
func (s *QueryCheckpointScheduler) Start() {
	s.w.Run(s.loop)
}

// Stop signals the scheduler to stop and waits for it to finish.
func (s *QueryCheckpointScheduler) Stop() {
	s.w.Stop()
}

// loop is the main scheduler loop.
func (s *QueryCheckpointScheduler) loop(stop <-chan struct{}) {
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

		cronExpr := s.getSchedule()
		if cronExpr == "" {
			timer = nil

			return nil
		}

		schedule, err := processing.CronParser.Parse(cronExpr)
		if err != nil {
			s.logger.WithFields(map[string]any{
				"cron":  cronExpr,
				"error": err,
			}).Errorf("Invalid query checkpoint schedule cron expression, disabling scheduler")

			timer = nil

			return nil
		}

		nextFire := schedule.Next(time.Now())

		delay := max(time.Until(nextFire), 0)

		s.logger.WithFields(map[string]any{
			"cron":     cronExpr,
			"nextFire": nextFire.Format(time.RFC3339),
		}).Infof("Query checkpoint scheduler armed")

		timer = time.NewTimer(delay)

		return timer.C
	}

	timerCh := resetTimer()

	for {
		select {
		case <-stop:
			return
		case <-s.scheduleChanged.C():
			timerCh = resetTimer()
		case <-timerCh:
			if s.isLeader() {
				s.logger.Infof("Query checkpoint scheduler firing: proposing CreateQueryCheckpoint")
				s.proposeFn()
			}

			timerCh = resetTimer()
		}
	}
}
