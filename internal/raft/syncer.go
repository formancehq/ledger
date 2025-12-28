package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type syncer[State any, F FSM[State]] struct {
	spool             *spool
	fsm               F
	mu                sync.Mutex
	syncingContext    context.Context
	syncingCancel     func()
	syncingTerminated chan struct{}
	syncingError      error
	logger            logging.Logger
}

func (s *syncer[State, F]) CreateSnapshot(ctx context.Context) ([]byte, error) {
	return s.fsm.CreateSnapshot(ctx)
}

func (s *syncer[State, F]) RestoreSnapshot(ctx context.Context, leader uint64, data raftpb.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncingContext != nil {
		s.logger.Infof("Restoring snapshot while syncing - interupting actual syncing")
		s.syncingCancel()
		select {
		case <-s.syncingTerminated:
		case <-ctx.Done():
			return
		}
	}

	s.logger.Infof("Restoring snapshot - switching to syncing mode")
	s.syncingContext, s.syncingCancel = context.WithCancel(ctx)
	s.syncingTerminated = make(chan struct{})
	go func() {
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			s.syncingCancel()
			s.syncingContext = nil
			s.syncingCancel = nil
			close(s.syncingTerminated)
		}()
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok && errors.Is(err, context.Canceled) {
					s.logger.Infof("Snapshot restoration canceled")
					return
				}
				panic(r)
			}
		}()
		s.fsm.RestoreSnapshot(s.syncingContext, leader, data)

		for {
			s.mu.Lock()
			command, err := s.spool.Next()
			if err == io.EOF {
				if err := s.spool.Reset(); err != nil {
					panic(err)
				}
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
			if err != nil {
				panic(err)
			}

			_, err = s.fsm.ApplyEntries(s.syncingContext, command)
			if err != nil {
				s.syncingError = err
			}
		}

		if s.syncingError != nil {
			s.logger.Errorf("Snapshot restore failed: %v", s.syncingError)
		} else {
			s.logger.Infof("Snapshot restored - switching to normal mode")
		}
	}()
}

func (s *syncer[State, F]) ApplyEntries(ctx context.Context, commands ...Command) ([]ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.syncingContext != nil {
		s.logger.Debugf("Applying entries while syncing - appending to spool")
		if err := s.spool.AppendCommittedEntries(ctx, commands...); err != nil {
			return nil, fmt.Errorf("appending committed entries to spool: %w", err)
		}
		// Since: the rawNode is syncing, the rawNode is forcibly a follower,
		// so, we don't care about the result of applying the commands
		// because the commands are applied on the leader
		// so, we can return an empty slice
		return make([]ApplyResult, len(commands)), nil
	}
	if s.syncingError != nil {
		return nil, s.syncingError
	}

	return s.fsm.ApplyEntries(ctx, commands...)
}

func (s *syncer[State, F]) IsSyncing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.syncingContext != nil
}

func newSyncer[State any, F FSM[State]](spool *spool, fsm F, logger logging.Logger) *syncer[State, F] {
	return &syncer[State, F]{
		spool:  spool,
		fsm:    fsm,
		logger: logger,
	}
}
