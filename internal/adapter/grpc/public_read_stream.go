package grpc

import "google.golang.org/grpc"

// publicReadStream projects only delivered rows, after checkpoint selection and
// pagination. It preserves transport metadata, cancellation and send errors.
type publicReadStream[T any] struct {
	grpc.ServerStreamingServer[T]

	project func(*T) *T
}

func (s publicReadStream[T]) Send(item *T) error {
	return s.ServerStreamingServer.Send(s.project(item))
}
