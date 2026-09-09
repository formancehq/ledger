package grpc

import ggrpc "google.golang.org/grpc"

// readProjectionStream projects only messages leaving the adapter. Keeping the
// cursor intact preserves its pagination and upstream-trailer capabilities.
type readProjectionStream[T any] struct {
	ggrpc.ServerStreamingServer[T]

	project func(*T) (*T, error)
}

func (s *readProjectionStream[T]) Send(value *T) error {
	projected, err := s.project(value)
	if err != nil {
		return err
	}

	return s.ServerStreamingServer.Send(projected)
}
