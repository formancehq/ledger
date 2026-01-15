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
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
)

type syncerStatus int

const (
	syncerStatusNormal syncerStatus = iota
	syncerStatusSyncing
)

// todo: handle the case where the service is restarted when the spool is not empty
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

func (s *syncer) ApplyEntries(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) ([]ApplyResult, error) {

	switch s.status.Load() {
	case syncerStatusNormal:
		return s.applyEntries(ctx, confState, entries...)
	case syncerStatusSyncing:
		s.logger.Debugf("Spool committed entries")
		s.mu.Lock()
		err := s.spool.AppendCommittedEntries(ctx, entries...)
		s.mu.Unlock()
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
	status := s.status.Load()
	if status == syncerStatusSyncing {
		s.logger.Infof("Interrupting previous sync")
		s.taskExecutor.interrupt()
	}
	s.status.Store(syncerStatusSyncing)

	s.taskExecutor.run(ctx, func(ctx context.Context) error {
		defer func() {
			s.logger.Infof("Syncing snapshot terminated")
		}()
		err := s.fsm.SyncSnapshot(ctx, leader, snapshot)
		if err != nil {
			return err
		}

		s.logger.Infof("Snapshot synced from leader, applying spooled entries...")

		applyEntries := func(entries ...raftpb.Entry) error {
			s.logger.WithFields(map[string]any{
				"count":    len(entries),
				"maxIndex": entries[len(entries)-1].Index,
			}).Infof("Applying spooled entries")
			_, err := s.applyEntries(ctx, nil, entries...)
			if err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
			return err
		}

		// todo: this way of doing cause massive back and forth on the underlying file
		// I guess we could use a pipe here instead to write to the file, while writing in memory
		const batchSize = 10000
		batch := make([]raftpb.Entry, 0, batchSize)
		count := 0
		for {

			select {
			// Check for context cancellation
			case <-ctx.Done():
				s.logger.Infof("Context cancelled, stopping unspooling")
				return ctx.Err()
			default:
			}

			s.mu.Lock()
			spooledEntry, err := s.spool.Next()
			s.mu.Unlock()

			if err != nil && !errors.Is(err, io.EOF) {
				panic(err)
			}
			if errors.Is(err, io.EOF) {
				s.mu.Lock()
				if len(batch) > 0 {
					if err := applyEntries(batch...); err != nil {
						return err
					}
					count += len(batch)
				}
				// todo: reset later, we have a pointer to the last read entry
				s.logger.WithFields(map[string]any{
					"totalCount": count,
				}).Infof("No more entries in spool, resetting")
				err := s.spool.Reset()
				s.status.Store(syncerStatusNormal)
				s.mu.Unlock()
				if err != nil {
					return err
				}
				break
			}
			batch = append(batch, *spooledEntry)

			if len(batch) >= batchSize {
				if err := applyEntries(batch...); err != nil {
					return err
				}
				count += len(batch)
				batch = batch[:0]
			}
		}
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
