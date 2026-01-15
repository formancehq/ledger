package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
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
)

type syncer struct {
	mu                      sync.Mutex
	spool                   Spool
	fsm                     FSM
	logger                  logging.Logger
	createSnapshotHistogram metric.Float64Histogram

	status            atomic.Value
	wal               WAL
	snapshotThreshold uint64
	compactionMargin  uint64
	taskExecutor      *singleTaskExecutor
}

func (s *syncer) ApplyEntries(ctx context.Context, index uint64, confState *raftpb.ConfState, commands ...*ledgerpb.Command) ([]ApplyResult, error) {

	switch s.status.Load() {
	case syncerStatusNormal:
		entries, err := s.fsm.ApplyEntries(ctx, commands...)
		if err != nil {
			panic(err)
		}

		lastSnapshot, err := s.wal.Snapshot()
		if err != nil {
			panic(fmt.Errorf("getting last snapshot: %w", err))
		}

		if index-lastSnapshot.Metadata.Index >= s.snapshotThreshold {
			s.logger.WithFields(map[string]any{
				"applied":           index,
				"lastSnapshotIndex": lastSnapshot.Metadata.Index,
				"snapshotThreshold": s.snapshotThreshold,
				"compactionMargin":  s.compactionMargin,
			}).Infof("Creating new snapshot")

			startTime := time.Now()
			data, err := s.fsm.CreateSnapshot(ctx)
			if err != nil {
				return nil, err
			}
			err = s.wal.CreateSnapshot(index, confState, data)
			if err != nil {
				return nil, err
			}
			duration := time.Since(startTime)
			s.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))

			// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
			err = s.wal.Compact(index - s.compactionMargin)
			if err != nil {
				return nil, err
			}
		}

		return entries, nil
	case syncerStatusSyncing:
		s.logger.Infof("Spool committed entries")
		s.mu.Lock()
		err := s.spool.AppendCommittedEntries(ctx, commands...)
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return make([]ApplyResult, len(commands)), nil
	default:
		panic("unreachable")
	}
}

func (s *syncer) SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	status := s.status.Load()
	if status == syncerStatusSyncing {
		s.taskExecutor.interrupt()
	}
	s.status.Store(syncerStatusSyncing)

	s.taskExecutor.run(ctx, func(ctx context.Context) error {
		err := s.fsm.SyncSnapshot(ctx, leader, snapshot)
		if err != nil {
			return err
		}

		for {
			s.logger.Infof("Unspooling...")

			s.mu.Lock()
			spooledCmd, err := s.spool.Next()
			s.mu.Unlock()

			if err != nil && !errors.Is(err, io.EOF) {
				panic(err)
			}
			if errors.Is(err, io.EOF) {
				s.logger.Infof("No more entries in spool, resetting")
				s.mu.Lock()
				err := s.spool.Reset()
				s.status.Store(syncerStatusNormal)
				s.mu.Unlock()
				if err != nil {
					return err
				}
				break
			}

			_, err = s.fsm.ApplyEntries(context.Background(), spooledCmd)
			if err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
		}

		return nil
	})

	return nil
}

func (s *syncer) IsSyncing() bool {
	return s.status.Load() == syncerStatusSyncing
}

func newSyncer(
	spool Spool,
	fsm FSM,
	logger logging.Logger,
	wal WAL,
	meter metric.Meter,
	snapshotThreshold, compactionMargin uint64,
) *syncer {
	initialStatus := atomic.Value{}
	initialStatus.Store(syncerStatusNormal)
	s := &syncer{
		snapshotThreshold: snapshotThreshold,
		compactionMargin:  compactionMargin,
		spool:             spool,
		fsm:               fsm,
		logger:            logger,
		status:            initialStatus,
		wal:               wal,
		taskExecutor:      newSingleTaskExecutor(logger),
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
	ctx        context.Context
	cancel     context.CancelFunc
	terminated chan struct{}
	logger     logging.Logger
}

func (t *singleTaskExecutor) run(ctx context.Context, fn func(ctx context.Context) error) {
	select {
	case <-t.terminated:
		t.terminated = make(chan struct{})
		t.ctx, t.cancel = context.WithCancel(ctx)

		otlplogs.Go(func() {
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
	}
}

func newSingleTaskExecutor(logger logging.Logger) *singleTaskExecutor {
	terminatedChan := make(chan struct{})
	close(terminatedChan)
	return &singleTaskExecutor{
		terminated: terminatedChan,
		logger:     logger,
	}
}
