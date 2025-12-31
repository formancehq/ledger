package raft

import (
	"context"
	"os"

	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type QueueObserver[T any] struct {
	queue        Queue[T]
	logger       logging.Logger
	incoming     metric.Float64Counter
	outgoing     metric.Float64Counter
	inflight     metric.Float64UpDownCounter
	fullCounter  metric.Float64Counter
	attributesFn func(t T) []attribute.KeyValue
	meter        metric.Meter
	name         string
	out          chan T
}

func (m *QueueObserver[T]) Send(msg T) bool {
	if m.queue.Send(msg) {
		m.incoming.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(m.attributesFn(msg)...)))
		m.inflight.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(m.attributesFn(msg)...)))
		return true
	} else {
		m.logger.WithFields(map[string]any{
			"channel": m.name,
		}).Errorf("Channel full")
		m.fullCounter.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(m.attributesFn(msg)...)))
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
	queue Queue[T],
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
	ret.incoming, err = ret.meter.Float64Counter(name+".incoming", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	ret.outgoing, err = ret.meter.Float64Counter(name+".outgoing", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	ret.inflight, err = ret.meter.Float64UpDownCounter(name+".inflight", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	ret.fullCounter, err = ret.meter.Float64Counter(name+".full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	go func() {
		for msg := range queue.Recv() {
			ret.out <- msg
			ret.outgoing.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(ret.attributesFn(msg)...)))
			ret.inflight.Add(context.Background(), -1, metric.WithAttributeSet(attribute.NewSet(ret.attributesFn(msg)...)))
		}
	}()

	return ret
}

type QueueObserverOption[T any] func(ch *QueueObserver[T])

func WithAttributesFn[T any](fn func(t T) []attribute.KeyValue) QueueObserverOption[T] {
	return func(ch *QueueObserver[T]) {
		ch.attributesFn = fn
	}
}

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
		WithAttributesFn(func(t T) []attribute.KeyValue {
			return nil
		}),
		WithMeter[T](noop.Meter{}),
		WithLogger[T](logging.NewDefaultLogger(os.Stdout, false, false, false)),
	}
}
