package raft

import (
	"context"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
)

type syncer[State any, F FSM[State]] struct {
	spool   *spool
	fsm     F
	mu      sync.Mutex
	syncing bool
	logger  logging.Logger
}

func (s *syncer[State, F]) CreateSnapshot(ctx context.Context) ([]byte, error) {
	return s.fsm.CreateSnapshot(ctx)
}

func (s *syncer[State, F]) RestoreSnapshot(ctx context.Context, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncing {
		// todo: handle the case
		panic("cannot restore snapshot while syncing")
	}
	s.logger.Infof("Restoring snapshot - switching to syncing mode")
	s.syncing = true
	go func() {
		s.fsm.RestoreSnapshot(ctx, data)

		for {
			s.mu.Lock()
			command, err := s.spool.Next()
			if err == io.EOF {
				if err := s.spool.Reset(); err != nil {
					panic(err)
				}
				s.syncing = false
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
			if err != nil {
				panic(err)
			}

			_ = s.fsm.ApplyEntries(ctx, command)
		}

		s.logger.Infof("Snapshot restored - switching to normal mode")
		s.syncing = false
	}()
}

func (s *syncer[State, F]) ApplyEntries(ctx context.Context, commands ...Command) []ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncing {
		s.logger.Debugf("Applying entries while syncing - appending to spool")
		if err := s.spool.AppendCommittedEntries(ctx, commands...); err != nil {
			panic(err)
		}
		// Since: the node is syncing, the node is forcibly a follower,
		// so we don't care about the result of applying the commands
		// because the commands are applied on the leader
		return make([]ApplyResult, 0)
	}

	return s.fsm.ApplyEntries(ctx, commands...)
}

func (s *syncer[State, F]) IsSyncing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.syncing
}

func newSyncer[State any, F FSM[State]](spool *spool, fsm F, logger logging.Logger) *syncer[State, F] {
	return &syncer[State, F]{
		spool:  spool,
		fsm:    fsm,
		logger: logger,
	}
}
