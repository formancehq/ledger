package batching

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/utils/job"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
)

type OnBatchProcessed[T any] func(...T)

func NoOpOnBatchProcessed[T any]() func(...T) {
	return func(t ...T) {}
}

type pending[T any] struct {
	object   T
	callback func()
}

type batcherJob[T any] struct {
	items            []*pending[T]
	onBatchProcessed OnBatchProcessed[T]
}

func (b batcherJob[T]) String() string {
	return fmt.Sprintf("processing %d items", len(b.items))
}

func (b batcherJob[T]) Terminated() {
	for _, v := range b.items {
		v.callback()
	}
	if b.onBatchProcessed != nil {
		b.onBatchProcessed(collectionutils.Map(b.items, func(from *pending[T]) T {
			return from.object
		})...)
	}
}

type Batcher[T any] struct {
	*job.Runner[batcherJob[T]]
	pending          []*pending[T]
	mu               sync.Mutex
	maxBatchSize     int
	onBatchProcessed OnBatchProcessed[T]
}

func (s *Batcher[T]) Append(object T, callback func()) {
	s.mu.Lock()
	s.pending = append(s.pending, &pending[T]{
		callback: callback,
		object:   object,
	})
	s.mu.Unlock()
	s.Runner.Next()
}

func (s *Batcher[T]) nextBatch() *batcherJob[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return nil
	}
	if len(s.pending) > s.maxBatchSize {
		batch := s.pending[:s.maxBatchSize]
		s.pending = s.pending[s.maxBatchSize:]
		return &batcherJob[T]{
			onBatchProcessed: s.onBatchProcessed,
			items:            batch,
		}
	}
	batch := s.pending
	s.pending = make([]*pending[T], 0)
	return &batcherJob[T]{
		items:            batch,
		onBatchProcessed: s.onBatchProcessed,
	}
}

func NewBatcher[T any](runner func(context.Context, ...T) error, onBatchProcessed OnBatchProcessed[T], nbWorkers, maxBatchSize int) *Batcher[T] {
	ret := &Batcher[T]{
		maxBatchSize:     maxBatchSize,
		onBatchProcessed: onBatchProcessed,
	}
	ret.Runner = job.NewJobRunner[batcherJob[T]](func(ctx context.Context, job *batcherJob[T]) error {
		return runner(ctx, collectionutils.Map(job.items, func(from *pending[T]) T {
			return from.object
		})...)
	}, ret.nextBatch, nbWorkers)
	return ret
}
