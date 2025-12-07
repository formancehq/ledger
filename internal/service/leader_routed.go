package service

import (
	"context"

	"go.etcd.io/etcd/raft/v3"
)

// LeaderRouted routes between two implementations based on whether the node is the leader
type LeaderRouted[T any] struct {
	node      *raft.RawNode
	ifLeader  T
	notLeader T
}

func (l *LeaderRouted[T]) Get(ctx context.Context) T {
	if l.node.Status().RaftState == raft.StateLeader {
		return l.ifLeader
	}
	return l.notLeader
}

func NewLeaderRouted[T any](node *raft.RawNode, ifLeader T, notLeader T) *LeaderRouted[T] {
	return &LeaderRouted[T]{
		node:      node,
		ifLeader:  ifLeader,
		notLeader: notLeader,
	}
}