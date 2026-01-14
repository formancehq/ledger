package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source syncer.go -destination syncer_generated_test.go -package raft . FSM
type FSM interface {
	CreateSnapshot(ctx context.Context) ([]byte, error)
	SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error
	ApplyEntries(ctx context.Context, commands ...*ledgerpb.Command) ([]ApplyResult, error)
}

type createSnapshotTerminatedCommand struct {
}
type syncSnapshotCommand struct {
	leader   uint64
	snapshot raftpb.Snapshot
}

type unspoolCommand struct {
	syncSnapshotCommand syncSnapshotCommand
}

type applyEntriesCommand struct {
	commands     []*ledgerpb.Command
	resultCh     chan []ApplyResult
	errCh        chan error
	appliedIndex uint64
	confState    *raftpb.ConfState
}

type syncerStatus int

const (
	syncerStatusNormal syncerStatus = iota
	syncerStatusSyncing
	syncerStatusSnapshotting
)

type syncer struct {
	spool                   Spool
	fsm                     FSM
	logger                  logging.Logger
	createSnapshotHistogram metric.Float64Histogram

	status            syncerStatus
	commands          chan any
	wal               *WAL
	stopCh            chan chan struct{}
	snapshotThreshold uint64
	compactionMargin  uint64
}

func (s *syncer) run() {

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
			case createSnapshotTerminatedCommand:
				s.status = syncerStatusNormal
			case syncSnapshotCommand:
				switch s.status {
				case syncerStatusNormal:
				case syncerStatusSnapshotting:
					s.logger.Infof("Snapshotting in progress, interrupting")
					taskExecutor.interrupt()
				case syncerStatusSyncing:
					taskExecutor.interrupt()
				}
				s.status = syncerStatusSyncing

				taskExecutor.run(context.Background(), func(ctx context.Context) (any, error) {
					err := s.fsm.SyncSnapshot(ctx, cmd.leader, cmd.snapshot)
					if err != nil {
						return nil, err
					}
					return unspoolCommand{cmd}, nil
				})

			case unspoolCommand:
				s.logger.Infof("Unspooling...")
				spooledCmd, err := s.spool.Next()
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}
				if errors.Is(err, io.EOF) {
					s.logger.Infof("No more entries in spool, resetting")
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

				otlplogs.Go(func() {
					s.commands <- cmd
				}, s.logger)

			case applyEntriesCommand:
				switch s.status {
				case syncerStatusNormal, syncerStatusSnapshotting:
					entries, err := s.fsm.ApplyEntries(context.Background(), cmd.commands...)
					if err != nil {
						panic(err)
					}
					cmd.resultCh <- entries

					// Check if we need to create a snapshot
					lastSnapshot, err := s.wal.Snapshot()
					if err != nil {
						panic(fmt.Errorf("getting last snapshot: %w", err))
					}

					if cmd.appliedIndex-lastSnapshot.Metadata.Index > s.snapshotThreshold && s.status == syncerStatusNormal {
						s.logger.WithFields(map[string]any{
							"applied":           cmd.appliedIndex,
							"lastSnapshotIndex": lastSnapshot.Metadata.Index,
							"snapshotThreshold": s.snapshotThreshold,
							"compactionMargin":  s.compactionMargin,
						}).Infof("Creating new snapshot")
						s.status = syncerStatusSnapshotting

						taskExecutor.run(
							context.Background(),
							func(ctx context.Context) (any, error) {
								startTime := time.Now()
								data, err := s.fsm.CreateSnapshot(ctx)
								if err == nil {
									_, err = s.wal.CreateSnapshot(cmd.appliedIndex, cmd.confState, data)
								}

								// Record metric for snapshot creation duration
								if err == nil {
									duration := time.Since(startTime)
									s.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))
								}

								// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
								err = s.wal.Compact(cmd.appliedIndex - s.compactionMargin)
								if err != nil {
									panic("Compacting storage failed: " + err.Error())
								}

								return createSnapshotTerminatedCommand{}, err
							},
						)
					}
				case syncerStatusSyncing:
					s.logger.Infof("Spool committed entries")
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

func (s *syncer) ApplyEntries(ctx context.Context, index uint64, confState *raftpb.ConfState, commands ...*ledgerpb.Command) ([]ApplyResult, error) {
	cmd := applyEntriesCommand{
		commands:     commands,
		resultCh:     make(chan []ApplyResult, 1),
		errCh:        make(chan error, 1),
		appliedIndex: index,
		confState:    confState,
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

func (s *syncer) SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	cmd := syncSnapshotCommand{
		leader:   leader,
		snapshot: snapshot,
	}

	select {
	case s.commands <- cmd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *syncer) IsSyncing() bool {
	return s.status == syncerStatusSyncing
}

func (s *syncer) stop() {
	ch := make(chan struct{})
	s.stopCh <- ch
	<-ch
}

func newSyncer(
	spool Spool,
	fsm FSM,
	logger logging.Logger,
	wal *WAL,
	meter metric.Meter,
	snapshotThreshold, compactionMargin uint64,
) *syncer {
	s := &syncer{
		snapshotThreshold: snapshotThreshold,
		compactionMargin:  compactionMargin,
		spool:             spool,
		fsm:               fsm,
		logger:            logger,
		status:            0,
		commands:          make(chan any, 1),
		wal:               wal,
		stopCh:            make(chan chan struct{}, 1),
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

func (t *singleTaskExecutor) run(ctx context.Context, fn func(ctx context.Context) (any, error)) {
	select {
	case <-t.terminated:
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)

		otlplogs.Go(func() {
			defer func() {
				t.cancel()
				close(t.terminated)
			}()

			next, err := fn(t.ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				panic(err)
			}

			t.nextChannel <- next
		}, t.logger)
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
