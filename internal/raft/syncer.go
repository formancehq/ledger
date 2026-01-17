package raft

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
)

const (
	syncerStatusNormal = iota
	syncerStatusSyncing
)

// todo: handle the case where the service is restarted when the spool is not empty
type syncer struct {
	spool                   Spool
	fsm                     FSM
	store                   Store
	logger                  logging.Logger
	createSnapshotHistogram metric.Float64Histogram

	status            *atomic.Int32
	wal               WAL
	snapshotThreshold uint64
	compactionMargin  uint64
	taskExecutor      *singleTaskExecutor
	syncTerminated    chan struct{}
}

func (s *syncer) ApplyEntries(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) ([]ApplyResult, error) {
	select {
	case <-s.syncTerminated:
		s.logger.Infof("Syncing terminated, applying spooled entries before resuming...")
		s.syncTerminated = nil
		position, err := s.spool.End()
		if err != nil {
			return nil, fmt.Errorf("getting spool end position: %w", err)
		}
		lastAppliedIndex, err := s.store.GetLastAppliedIndex()
		if err != nil {
			return nil, fmt.Errorf("getting last applied index: %w", err)
		}

		if err := s.spool.ReplayUntil(ctx, *position, lastAppliedIndex, func(entry raftpb.Entry) error {
			_, err := s.applyEntries(ctx, confState, entry)
			return err
		}); err != nil {
			return nil, fmt.Errorf("replaying spool: %w", err)
		}

		if err := s.spool.Prune(lastAppliedIndex); err != nil {
			return nil, fmt.Errorf("pruning spool: %w", err)
		}

		s.status.Store(syncerStatusNormal)
	default:
	}

	switch s.status.Load() {
	case syncerStatusNormal:
		return s.applyEntries(ctx, confState, entries...)
	case syncerStatusSyncing:
		s.logger.Debugf("Spool committed entries")
		err := s.spool.AppendCommittedEntries(ctx, entries...)
		if err != nil {
			return nil, err
		}
		return make([]ApplyResult, len(entries)), nil
	default:
		panic("unreachable")
	}
}

func (s *syncer) SyncSnapshot(ctx context.Context, leader uint64, snapshot raftpb.Snapshot) error {
	s.logger.
		WithFields(map[string]any{
			"leader": leader,
			"index":  snapshot.Metadata.Index,
			"term":   snapshot.Metadata.Term,
		}).
		Infof("Syncing snapshot from leader")
	if s.status.Swap(syncerStatusSyncing) == syncerStatusSyncing {
		s.logger.Infof("Interrupting previous sync")
		s.taskExecutor.interrupt()
	}
	syncTerminated := make(chan struct{})
	s.syncTerminated = syncTerminated

	s.taskExecutor.run(ctx, func(ctx context.Context) error {
		defer func() {
			s.logger.Infof("Syncing snapshot terminated")
		}()
		err := s.fsm.SyncSnapshot(ctx, leader, snapshot)
		if err != nil {
			return err
		}

		s.logger.Infof("Snapshot synced from leader, applying spooled entries...")
		end, err := s.spool.End()
		if err != nil {
			return err
		}

		lastAppliedIndex, err := s.store.GetLastAppliedIndex()
		if err != nil {
			return err
		}

		err = s.spool.ReplayUntil(ctx, *end, lastAppliedIndex, func(entry raftpb.Entry) error {
			_, err := s.fsm.ApplyEntries(ctx, entry)
			return err
		})
		if err != nil {
			return err
		}

		close(syncTerminated)

		s.logger.Infof("Unspooling done")

		return nil
	})

	return nil
}

func (s *syncer) IsSyncing() bool {
	return s.status.Load() == syncerStatusSyncing
}

func (s *syncer) applyEntries(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) ([]ApplyResult, error) {
	results, err := s.fsm.ApplyEntries(ctx, entries...)
	if err != nil {
		panic(err)
	}

	lastSnapshot, err := s.wal.Snapshot()
	if err != nil {
		panic(fmt.Errorf("getting last snapshot: %w", err))
	}

	if entries[len(entries)-1].Index-lastSnapshot.Metadata.Index >= s.snapshotThreshold {
		s.logger.WithFields(map[string]any{
			"applied":           entries[len(entries)-1].Index,
			"lastSnapshotIndex": lastSnapshot.Metadata.Index,
			"snapshotThreshold": s.snapshotThreshold,
			"compactionMargin":  s.compactionMargin,
		}).Infof("Creating new snapshot")

		startTime := time.Now()
		data, err := s.fsm.CreateSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		err = s.wal.CreateSnapshot(entries[len(entries)-1].Index, confState, data)
		if err != nil {
			return nil, err
		}
		duration := time.Since(startTime)
		s.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))

		// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
		err = s.wal.Compact(entries[len(entries)-1].Index - s.compactionMargin)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (s *syncer) Replay(ctx context.Context) error {
	lastAppliedIndex, err := s.store.GetLastAppliedIndex()
	if err != nil {
		return err
	}

	end, err := s.spool.End()
	if err != nil {
		return err
	}

	return s.spool.ReplayUntil(ctx, *end, lastAppliedIndex, func(entry raftpb.Entry) error {
		_, err := s.fsm.ApplyEntries(ctx, entry)
		return err
	})
}

func newSyncer(
	spool Spool,
	fsm FSM,
	logger logging.Logger,
	wal WAL,
	meter metric.Meter,
	store Store,
	snapshotThreshold, compactionMargin uint64,
) *syncer {
	initialStatus := atomic.Int32{}
	initialStatus.Store(syncerStatusNormal)
	s := &syncer{
		snapshotThreshold: snapshotThreshold,
		compactionMargin:  compactionMargin,
		spool:             spool,
		fsm:               fsm,
		logger:            logger,
		status:            &initialStatus,
		wal:               wal,
		store:             store,
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
