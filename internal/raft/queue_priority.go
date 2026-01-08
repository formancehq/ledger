package raft

import (
	"reflect"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type PriorityQueue[T any] struct {
	queues     []chan T
	priorityFn func(T) int
	out        chan T
}

func (pq *PriorityQueue[T]) Send(msg T) bool {

	p := pq.priorityFn(msg)
	select {
	case pq.queues[p] <- msg:
		return true
	default:
		return false
	}
}

func (pq *PriorityQueue[T]) Recv() <-chan T {
	return pq.out
}

func (pq *PriorityQueue[T]) Close() {
	for _, ch := range pq.queues {
		close(ch)
	}
}

func NewPriorityQueue[T any](
	numberOfPriority int,
	priorityFn func(T) int,
	logger logging.Logger,
	options ...PriorityQueueOption[T],
) *PriorityQueue[T] {
	ret := &PriorityQueue[T]{
		queues:     make([]chan T, numberOfPriority),
		priorityFn: priorityFn,
		out:        make(chan T),
	}
	for _, opt := range append(defaultPriorityQueueOptions[T](), options...) {
		opt(ret)
	}

	otlplogs.Go(func() {
	l:
		for {
			for _, ch := range ret.queues {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					ret.out <- msg
					continue l
				default:
				}
			}

			// Not performant, but if we are here, we are not under load
			_, recv, ok := reflect.Select(
				collectionutils.Map(ret.queues, func(ch chan T) reflect.SelectCase {
					return reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ch),
					}
				}),
			)
			if ok {
				ret.out <- recv.Interface().(T)
			}
		}
	}, logger)

	return ret
}

type PriorityQueueOption[T any] func(queue *PriorityQueue[T])

func WithPriorityQueueSize[T any](size int) PriorityQueueOption[T] {
	return func(ch *PriorityQueue[T]) {
		for i := range ch.queues {
			ch.queues[i] = make(chan T, size)
		}
	}
}

func defaultPriorityQueueOptions[T any]() []PriorityQueueOption[T] {
	return []PriorityQueueOption[T]{
		WithPriorityQueueSize[T](100),
	}
}

func RaftMessagePriority(msg raftpb.Message) int {
	switch msg.Type {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		return 0
	case raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return 1
	case raftpb.MsgApp:
		if len(msg.Entries) == 0 {
			return 2
		}
		return 3
	default:
		return 4
	}
}
