package raft

import (
	"reflect"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type PriorityQueue[T any] struct {
	queues     []Queue[T]
	priorityFn func(T) int
	out        chan T
}

func (pq *PriorityQueue[T]) Push(msg T) bool {
	p := pq.priorityFn(msg)
	if p > len(pq.queues)-1 {
		p = len(pq.queues) - 1
	}
	return pq.queues[p].Push(msg)
}

func (pq *PriorityQueue[T]) Recv() <-chan T {
	return pq.out
}

func (pq *PriorityQueue[T]) Close() {
	for _, ch := range pq.queues {
		ch.Close()
	}
}

func NewPriorityQueue[T any](
	priorityFn func(T) int,
	logger logging.Logger,
	queues ...Queue[T],
) *PriorityQueue[T] {
	if len(queues) == 0 {
		panic("no queues provided")
	}

	ret := &PriorityQueue[T]{
		queues:     queues,
		priorityFn: priorityFn,
		out:        make(chan T),
	}

	otlplogs.Go(func() {
	l:
		for {
			for _, ch := range ret.queues {
				select {
				case msg, ok := <-ch.Recv():
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
				collectionutils.Map(ret.queues, func(ch Queue[T]) reflect.SelectCase {
					return reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ch.Recv()),
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

func CreateQueues[T any](queueConfigs []int, factory func(size, index int) Queue[T]) []Queue[T] {
	ret := make([]Queue[T], 0, len(queueConfigs))
	for i, queueSize := range queueConfigs {
		ret = append(ret, factory(queueSize, i))
	}
	return ret
}
