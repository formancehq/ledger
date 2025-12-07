package http

import (
	"go.etcd.io/etcd/raft/v3"
)

// isLeader checks if the current node is the leader
func (s *Server) isLeader() bool {
	if s.cluster == nil {
		return false
	}
	raftInstance := s.cluster.GetRaft()
	if raftInstance == nil {
		return false
	}
	status := raftInstance.Status()
	return status.RaftState == raft.StateLeader
}

