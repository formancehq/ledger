package leadership

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
)

// ServiceHandler represents a service that can be run and stopped when the leadership is acquired or lost
// on the instance.
// As the underlying mechanism is backed by a pg connection, the service handler MUST use the provided db
// to call the database.
// This ensures a minimal synchronization between the leadership and the capacity to read/write to the database.
type ServiceHandler interface {
	Run(ctx context.Context, db *DatabaseHandle)
	Stop(ctx context.Context) error
}

type Service struct {
	handler     ServiceHandler
	manager     *Manager
	stopChannel chan chan struct{}
	logger      logging.Logger
}

func (s *Service) Run(ctx context.Context) {
	signal := s.manager.GetBroadcaster()
	subscription, release := signal.Subscribe()
	defer release()

	isRunning := false

	for {
		select {
		case ch := <-s.stopChannel:
			if isRunning {
				if err := s.handler.Stop(ctx); err != nil {
					s.logger.Errorf("error stopping handler: %v", err)
				}
			}
			close(ch)
			return
		case l := <-subscription:
			if l.Acquired {
				isRunning = true
				go s.handler.Run(ctx, l.DB)
			} else if isRunning {
				if err := s.handler.Stop(ctx); err != nil {
					s.logger.Errorf("error stopping handler: %v", err)
				}
				isRunning = false
			}
		}
	}
}

func (s *Service) Stop(ctx context.Context) {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		s.logger.Error("error stopping service: context done")
		return
	case s.stopChannel <- ch:
		select {
		case <-ctx.Done():
			s.logger.Error("error stopping service: context done")
		case <-ch:
		}
	}
}

func NewService(manager *Manager, logger logging.Logger, handler ServiceHandler) *Service {
	return &Service{
		manager:     manager,
		handler:     handler,
		logger:      logger,
		stopChannel: make(chan chan struct{}),
	}
}
