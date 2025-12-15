package raft

import (
	"context"
	"sync/atomic"

	"github.com/formancehq/go-libs/v3/logging"
)

type syncer[State any, F FSM[State]] struct {
	spool   *spool
	fsm     F
	syncing atomic.Bool
	logger logging.Logger
}

func (s *syncer[State, F]) CreateSnapshot(ctx context.Context) ([]byte, error) {
	return s.fsm.CreateSnapshot(ctx)
}

func (s *syncer[State, F]) RestoreSnapshot(ctx context.Context, data []byte) {
	if s.syncing.Load() {
		// todo: handle the case
		panic("cannot restore snapshot while syncing")
	}
	s.logger.Infof("Restoring snapshot - switching to syncing mode")
	s.syncing.Store(true)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				panic(e)
			}
			s.logger.Infof("Snapshot restored - switching to normal mode")
			s.syncing.Store(false)
		}()

		s.fsm.RestoreSnapshot(ctx, data)
	}()
}

func (s *syncer[State, F]) ApplyEntries(ctx context.Context, commands ...Command) []ApplyResult {
	if s.syncing.Load() {
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

func newSyncer[State any, F FSM[State]](spool *spool, fsm F, logger logging.Logger) *syncer[State, F] {
	return &syncer[State, F]{
		spool: spool,
		fsm:   fsm,
		logger: logger,
	}
}