package raft

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
)

type Queue[T any] interface {
	Send(msg T) bool
	Recv() <-chan T
	Close()
}

func AddTypeAsAttribute(msg raftpb.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("type", msg.Type.String()),
	}
}