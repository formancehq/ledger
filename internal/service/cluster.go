package service

type Cluster interface {
	IsHealthy() bool
	GetLeader() uint64
}