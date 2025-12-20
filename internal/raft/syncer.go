package raft

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
)

type syncer[State any, F FSM[State]] struct {
	spool        *spool
	fsm          F
	mu           sync.Mutex
	syncing      bool
	syncingError error
	logger       logging.Logger
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

			_, err = s.fsm.ApplyEntries(ctx, command)
			if err != nil {
				s.syncingError = err
			}
		}

		if s.syncingError != nil {
			s.logger.Errorf("Snapshot restore failed: %v", s.syncingError)
		} else {
			s.logger.Infof("Snapshot restored - switching to normal mode")
		}
		s.syncing = false
	}()
}

func (s *syncer[State, F]) ApplyEntries(ctx context.Context, commands ...Command) ([]ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncing {
		s.logger.Debugf("Applying entries while syncing - appending to spool")
		if err := s.spool.AppendCommittedEntries(ctx, commands...); err != nil {
			return nil, fmt.Errorf("appending committed entries to spool: %w", err)
		}
		// Since: the rawNode is syncing, the rawNode is forcibly a follower,
		// so we don't care about the result of applying the commands
		// because the commands are applied on the leader
		return make([]ApplyResult, 0), nil
	}
	if s.syncingError != nil {
		return nil, s.syncingError
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
