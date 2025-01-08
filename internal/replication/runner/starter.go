package runner

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/replication/signal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source starter.go -destination starter_generated_test.go -package runner . Leadership

type Leadership interface {
	GetLeadership() *signal.Signal[bool]
}

type Starter struct {
	leadership Leadership
	runner     *Runner
	logger     logging.Logger
	store      SystemStore
}

func (s *Starter) restorePipelines(ctx context.Context) error {
	s.logger.Info("restore states from store")
	states, err := s.store.ListEnabledPipelines(ctx)
	if err != nil {
		return fmt.Errorf("reading states from store: %w", err)
	}

	for _, state := range states {
		if _, err := s.runner.StartPipeline(ctx, state); err != nil {
			return err
		}
	}

	return nil
}

func (s *Starter) Run(ctx context.Context) {
	listener, cancel := s.leadership.GetLeadership().Listen()
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case isLeader := <-listener:
			if isLeader {
				if err := s.runner.StartAsync(ctx); err != nil {
					panic(err)
				}
				if err := s.restorePipelines(ctx); err != nil {
					panic(err)
				}

				<-ctx.Done()
			}
		}
	}
}

func NewStarter(
	leadership Leadership,
	runner *Runner,
	logger logging.Logger,
	store SystemStore,
) *Starter {
	return &Starter{
		leadership: leadership,
		runner:     runner,
		logger:     logger,
		store:      store,
	}
}
