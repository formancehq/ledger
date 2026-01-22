package raft

import (
	"context"
	"math"
	"os"
	"slices"
	"sync/atomic"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func yConcave(x, xMax, yMax, p float64) float64 {
	if x < 0 || xMax <= 0 || p <= 0 {
		return math.NaN()
	}
	t := math.Log(x+1) / math.Log(xMax+1)
	return yMax * math.Pow(t, p)
}

func logBoundaries(numberOfBuckets, bucketMaxValue int) []float64 {
	ret := make([]float64, numberOfBuckets)
	for i := range numberOfBuckets {
		ret[i] = math.Floor(yConcave(float64(i), float64(numberOfBuckets-1), float64(bucketMaxValue), 1))
	}
	return slices.Compact(ret)
}

type QueueObserver[T any] struct {
	queue           Queue[T]
	logger          logging.Logger
	histogram       metric.Int64Histogram
	fullCounter     metric.Float64Counter
	meter           metric.Meter
	name            string
	out             chan T
	inflightCounter atomic.Int32
}

func (m *QueueObserver[T]) Push(msg T) bool {
	if m.queue.Push(msg) {
		m.histogram.Record(context.Background(), int64(m.inflightCounter.Add(1)))
		return true
	} else {
		m.logger.WithFields(map[string]any{
			"channel": m.name,
		}).Errorf("Channel full")
		m.fullCounter.Add(context.Background(), 1)
		return false
	}
}

func (m *QueueObserver[T]) Recv() <-chan T {
	return m.out
}

func (m *QueueObserver[T]) Close() {
	m.queue.Close()
}

func NewQueueObserver[T any](
	name string,
	queue interface {
		Queue[T]
		Capacity
	},
	options ...QueueObserverOption[T],
) *QueueObserver[T] {

	ret := &QueueObserver[T]{
		name:  name,
		queue: queue,
		out:   make(chan T),
	}

	for _, option := range append(defaultQueueObserverOptions[T](), options...) {
		option(ret)
	}

	var err error
	ret.fullCounter, err = ret.meter.Float64Counter(name+".full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	ret.histogram, err = ret.meter.Int64Histogram(
		name+".load",
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			logBoundaries(12, queue.Capacity())...,
		),
	)
	if err != nil {
		panic(err)
	}

	otlplogs.Go(func() {
		for msg := range queue.Recv() {
			ret.out <- msg
			ret.inflightCounter.Add(-1)
		}
	}, ret.logger)

	return ret
}

type QueueObserverOption[T any] func(ch *QueueObserver[T])

func WithMeter[T any](meter metric.Meter) QueueObserverOption[T] {
	return func(ch *QueueObserver[T]) {
		ch.meter = meter
	}
}

func WithLogger[T any](logger logging.Logger) QueueObserverOption[T] {
	return func(ch *QueueObserver[T]) {
		ch.logger = logger
	}
}

func defaultQueueObserverOptions[T any]() []QueueObserverOption[T] {
	return []QueueObserverOption[T]{
		WithMeter[T](noop.Meter{}),
		WithLogger[T](logging.NewDefaultLogger(os.Stdout, false, false, false)),
	}
}
