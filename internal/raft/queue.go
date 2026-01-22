package raft

type Queue[T any] interface {
	Push(msg T) bool
	Recv() <-chan T
	Close()
}

type Capacity interface {
	Capacity() int
}