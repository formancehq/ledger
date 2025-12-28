package raft

import (
	"context"
	"os"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type Channel[T any] struct {
	in           chan T
	out          chan T
	logger       logging.Logger
	incoming     metric.Float64Counter
	outgoing     metric.Float64Counter
	fullCounter  metric.Float64Counter
	attributesFn func(t T) []attribute.KeyValue
	meter        metric.Meter
	name         string
}

func (m *Channel[T]) Send(msg T) bool {
	select {
	case m.in <- msg:
		m.incoming.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(m.attributesFn(msg)...)))
		return true
	default:
		m.logger.WithFields(map[string]any{
			"channel": m.name,
		}).Errorf("Channel full")
		m.fullCounter.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(m.attributesFn(msg)...)))
		return false
	}
}

func (m *Channel[T]) Recv() <-chan T {
	return m.out
}

func (m *Channel[T]) stop() {
	close(m.in)
}

func NewChannel[T any](
	name string,
	options ...ChannelOption[T],
) *Channel[T] {

	ret := &Channel[T]{
		name: name,
		out:  make(chan T, 1),
	}

	for _, option := range append(defaultChannelOptions[T](), options...) {
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

	ret.fullCounter, err = ret.meter.Float64Counter(name+".full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	go func() {
		for msg := range ret.in {
			ret.out <- msg
			ret.outgoing.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(ret.attributesFn(msg)...)))
		}
	}()

	return ret
}

type ChannelOption[T any] func(ch *Channel[T])

func WithSize[T any](size int) ChannelOption[T] {
	return func(ch *Channel[T]) {
		ch.in = make(chan T, size)
	}
}

func WithAttributesFn[T any](fn func(t T) []attribute.KeyValue) ChannelOption[T] {
	return func(ch *Channel[T]) {
		ch.attributesFn = fn
	}
}

func WithMeter[T any](meter metric.Meter) ChannelOption[T] {
	return func(ch *Channel[T]) {
		ch.meter = meter
	}
}

func WithLogger[T any](logger logging.Logger) ChannelOption[T] {
	return func(ch *Channel[T]) {
		ch.logger = logger
	}
}

func defaultChannelOptions[T any]() []ChannelOption[T] {
	return []ChannelOption[T]{
		WithSize[T](100),
		WithAttributesFn(func(t T) []attribute.KeyValue {
			return nil
		}),
		WithMeter[T](noop.Meter{}),
		WithLogger[T](logging.NewDefaultLogger(os.Stdout, false, false, false)),
	}
}

func AddTypeAsAttribute(msg raftpb.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("type", msg.Type.String()),
	}
}
