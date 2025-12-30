package raft

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type createSnapshotCommand struct {
	errCh   chan error
	applied uint64
	state   *raftpb.ConfState
}
type restoreSnapshotCommand struct {
	leader   uint64
	snapshot raftpb.Snapshot
	errCh    chan error
}

type restoreSnapshotUnspoolCommand struct{}

type applyEntriesCommand struct {
	commands []Command
	resultCh chan []ApplyResult
	errCh    chan error
}

type syncerStatus int

const (
	syncerStatusNormal syncerStatus = iota
	syncerStatusRestoring
	syncerStatusSnapshotting
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source syncer.go -destination syncer_generated_test.go -package raft . SnapshotStore
type SnapshotStore interface {
	CreateSnapshot(applied uint64, state *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
}

type syncer[State any, F FSM[State]] struct {
	spool  Spool
	fsm    F
	logger logging.Logger

	status   syncerStatus
	commands chan any
	storage  SnapshotStore
	stopCh   chan chan struct{}
}

func (s *syncer[State, F]) run() {

	taskExecutor := newSingleTaskExecutor()
	for {
		select {
		case stop := <-s.stopCh:
			taskExecutor.interrupt()
			close(stop)
			return
		case <-taskExecutor.success():
			s.status = syncerStatusNormal
		case cmd := <-s.commands:
			switch cmd := cmd.(type) {
			case createSnapshotCommand:
				switch s.status {
				case syncerStatusNormal:
					s.status = syncerStatusSnapshotting
				case syncerStatusRestoring:
					panic("snapshotting while restoring")
				case syncerStatusSnapshotting:
					s.logger.Infof("Snapshotting already in progress, interrupting")
					taskExecutor.interrupt()
				}

				taskExecutor.run(context.Background(), func(ctx context.Context) error {
					data, err := s.fsm.CreateSnapshot(ctx)
					if err != nil {
						cmd.errCh <- err
					} else {
						_, err = s.storage.CreateSnapshot(cmd.applied, cmd.state, data)
						if err != nil {
							cmd.errCh <- err
						} else {
							cmd.errCh <- nil
						}
					}
					return err
				})

			case restoreSnapshotCommand:
				switch s.status {
				case syncerStatusNormal:
					s.status = syncerStatusRestoring
				case syncerStatusSnapshotting:
					panic("restoring while snapshotting")
				case syncerStatusRestoring:
					taskExecutor.interrupt()
				}

				taskExecutor.run(context.Background(), func(ctx context.Context) error {
					return s.fsm.RestoreSnapshot(ctx, cmd.leader, cmd.snapshot)
				})

			case restoreSnapshotUnspoolCommand:
				spooledCmd, err := s.spool.Next()
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}
				if errors.Is(err, io.EOF) {
					if err := s.spool.Reset(); err != nil {
						panic(err)
					}
					s.status = syncerStatusNormal
					continue
				}

				_, err = s.fsm.ApplyEntries(context.Background(), spooledCmd)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
				if errors.Is(err, context.Canceled) {
					continue
				}

				go func() {
					s.commands <- restoreSnapshotUnspoolCommand{}
				}()

			case applyEntriesCommand:
				switch s.status {
				case syncerStatusNormal, syncerStatusSnapshotting:
					entries, err := s.fsm.ApplyEntries(context.Background(), cmd.commands...)
					if err != nil {
						panic(err)
					}
					cmd.resultCh <- entries
				case syncerStatusRestoring:
					err := s.spool.AppendCommittedEntries(context.Background(), cmd.commands...)
					if err != nil {
						cmd.errCh <- err
						continue
					}
					cmd.resultCh <- make([]ApplyResult, len(cmd.commands))
				}
			default:
				panic("unreachable")
			}
		}
	}
}

func (s *syncer[State, F]) CreateSnapshot(ctx context.Context, applied uint64, state *raftpb.ConfState) error {
	cmd := createSnapshotCommand{
		errCh:   make(chan error, 1),
		applied: applied,
		state:   state,
	}

	select {
	case s.commands <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-cmd.errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *syncer[State, F]) ApplyEntries(ctx context.Context, commands ...Command) ([]ApplyResult, error) {
	cmd := applyEntriesCommand{
		commands: commands,
		resultCh: make(chan []ApplyResult, 1),
		errCh:    make(chan error, 1),
	}

	select {
	case s.commands <- cmd:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case err := <-cmd.errCh:
		return nil, err
	case results := <-cmd.resultCh:
		return results, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *syncer[State, F]) RestoreSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	cmd := restoreSnapshotCommand{
		leader:   leader,
		snapshot: snapshot,
		errCh:    make(chan error, 1),
	}

	select {
	case s.commands <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-cmd.errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *syncer[State, F]) IsSyncing() bool {
	return s.status == syncerStatusRestoring
}

func (s *syncer[State, F]) stop() {
	ch := make(chan struct{})
	s.stopCh <- ch
	<-ch
}

func newSyncer[State any, F FSM[State]](
	spool Spool,
	fsm F,
	logger logging.Logger,
	store SnapshotStore,
) *syncer[State, F] {
	return &syncer[State, F]{
		spool:    spool,
		fsm:      fsm,
		logger:   logger,
		status:   0,
		commands: make(chan any, 1),
		storage:  store,
		stopCh:   make(chan chan struct{}, 1),
	}
}

type singleTaskExecutor struct {
	ctx            context.Context
	cancel         context.CancelFunc
	successfulJobs chan struct{}
	terminated     chan struct{}
}

func (t *singleTaskExecutor) run(ctx context.Context, fn func(ctx context.Context) error) {
	select {
	case <-t.terminated:
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)

		go func() {
			defer func() {
				t.cancel()
				close(t.terminated)
			}()

			err := fn(t.ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				panic(err)
			}

			t.successfulJobs <- struct{}{}
		}()
	default:
		panic("already running")
	}
}

func (t *singleTaskExecutor) interrupt() {
	select {
	case <-t.terminated:
	default:
		t.cancel()
		<-t.terminated
		select {
		case <-t.successfulJobs:
			// Drain potentially terminated job
		default:
		}
	}
}

func (t *singleTaskExecutor) success() chan struct{} {
	return t.successfulJobs
}

func newSingleTaskExecutor() *singleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)
	return &singleTaskExecutor{
		terminated:     terminatedChan,
		successfulJobs: make(chan struct{}, 1),
	}
}
