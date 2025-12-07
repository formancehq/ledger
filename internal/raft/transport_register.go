package raft

import (
	"google.golang.org/grpc"
)

// RegisterRaftTransportService registers the RaftTransportService on the given gRPC server
func RegisterRaftTransportService(server *grpc.Server, transport *Transport) {
	transport.RegisterRaftService(server)
}

