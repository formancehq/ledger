package raft

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type createSnapshotCommand struct {
	errCh   chan error
	applied uint64
	state   *raftpb.ConfState
}

type createSnapshotTerminatedCommand struct {
	createSnapshotCommand createSnapshotCommand
}
type restoreSnapshotCommand struct {
	leader   uint64
	snapshot raftpb.Snapshot
	errCh    chan error
}

type restoreSnapshotUnspoolCommand struct {
	restoreSnapshotCommand restoreSnapshotCommand
}

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
	spool                   Spool
	fsm                     F
	logger                  logging.Logger
	createSnapshotHistogram metric.Float64Histogram

	status   syncerStatus
	commands chan any
	storage  SnapshotStore
	stopCh   chan chan struct{}
}

func (s *syncer[State, F]) run() {

	defer otlplogs.RecoverAndLogPanics(s.logger)

	taskExecutor := newSingleTaskExecutor(s.logger, s.commands)
	for {
		select {
		case stop := <-s.stopCh:
			taskExecutor.interrupt()
			close(stop)
			return
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

				taskExecutor.run(
					context.Background(),
					cmd.errCh,
					func(ctx context.Context) (any, error) {
						startTime := time.Now()
						data, err := s.fsm.CreateSnapshot(ctx)
						if err == nil {
							_, err = s.storage.CreateSnapshot(cmd.applied, cmd.state, data)
						}

						// Record metric for snapshot creation duration
						if s.createSnapshotHistogram != nil && err == nil {
							duration := time.Since(startTime)
							s.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))
						}

						return createSnapshotTerminatedCommand{
							createSnapshotCommand: cmd,
						}, err
					},
				)
			case createSnapshotTerminatedCommand:
				s.status = syncerStatusNormal
			case restoreSnapshotCommand:
				switch s.status {
				case syncerStatusNormal:
					s.status = syncerStatusRestoring
				case syncerStatusSnapshotting:
					panic("restoring while snapshotting")
				case syncerStatusRestoring:
					taskExecutor.interrupt()
				}

				taskExecutor.run(context.Background(), cmd.errCh, func(ctx context.Context) (any, error) {
					err := s.fsm.RestoreSnapshot(ctx, cmd.leader, cmd.snapshot)
					if err != nil {
						return nil, err
					}
					return restoreSnapshotUnspoolCommand{cmd}, nil
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
					cmd.restoreSnapshotCommand.errCh <- nil
					continue
				}

				_, err = s.fsm.ApplyEntries(context.Background(), spooledCmd)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
				if errors.Is(err, context.Canceled) {
					cmd.restoreSnapshotCommand.errCh <- err
					continue
				}

				go func() {
					s.commands <- cmd
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
	meter metric.Meter,
) *syncer[State, F] {
	s := &syncer[State, F]{
		spool:    spool,
		fsm:      fsm,
		logger:   logger,
		status:   0,
		commands: make(chan any, 1),
		storage:  store,
		stopCh:   make(chan chan struct{}, 1),
	}

	// Create histogram metric for CreateSnapshot duration
	if meter == nil {
		meter = noop.Meter{}
	}
	histogram, err := meter.Float64Histogram("raft.syncer.create_snapshot.duration",
		metric.WithDescription("Time spent creating snapshot in syncer"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			// Fine-grained buckets for small values (0-100ms)
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			12, 15, 18, 20, 25, 30, 35, 40, 45, 50,
			60, 70, 80, 90, 100,
			// Medium buckets (100-500ms)
			125, 150, 175, 200, 250, 300, 350, 400, 450, 500,
			// Larger buckets (500ms-5s)
			600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 4000, 5000,
		),
	)
	if err == nil {
		s.createSnapshotHistogram = histogram
	}

	return s
}

type singleTaskExecutor struct {
	ctx         context.Context
	cancel      context.CancelFunc
	terminated  chan struct{}
	nextChannel chan any
	logger      logging.Logger
}

func (t *singleTaskExecutor) run(ctx context.Context, errCh chan error, fn func(ctx context.Context) (any, error)) {
	select {
	case <-t.terminated:
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)

		go func() {
			defer otlplogs.RecoverAndLogPanics(t.logger)
			defer func() {
				t.cancel()
				close(t.terminated)
			}()

			next, err := fn(t.ctx)
			errCh <- err
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				panic(err)
			}

			t.nextChannel <- next
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
		case <-t.nextChannel:
			// Drain channel
		default:
		}
	}
}

func newSingleTaskExecutor(logger logging.Logger, nextChannel chan any) *singleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)
	return &singleTaskExecutor{
		terminated:  terminatedChan,
		nextChannel: nextChannel,
		logger:      logger,
	}
}
