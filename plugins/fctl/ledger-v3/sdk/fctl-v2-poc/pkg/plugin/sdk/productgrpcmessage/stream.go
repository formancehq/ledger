package productgrpcmessage

import (
	"context"
	"io"
	"sync"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"google.golang.org/protobuf/proto"
)

// Stream consumes one server-streaming host request. A receive failure is
// terminal. Callers must withhold their overall result until a clean io.EOF.
type Stream struct {
	mu           sync.Mutex
	ctx          context.Context
	client       *Client
	responses    sdk.Responses
	terminal     error
	continuation string
	count        uint32
	aggregate    int64
}

// NewStream marshals the single request and performs exactly one Host.Request.
// Receive and cancellation remain governed by ctx and the host's Responses.
func (c *Client) NewStream(ctx context.Context, request proto.Message) (*Stream, error) {
	if !c.policy.GRPC.ServerStreaming {
		return nil, failure(sdk.FailureInvalidArgument, "invalid generated message stream")
	}
	responses, err := c.dispatch(ctx, request)
	if err != nil {
		return nil, err
	}
	return &Stream{ctx: ctx, client: c, responses: responses}, nil
}

// RecvInto decodes the next response into its exact descriptor-matching target.
// A failed receive leaves the target unchanged and is returned on later calls.
func (s *Stream) RecvInto(response proto.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal != nil {
		return s.terminal
	}
	if !matches(response, s.client.method.Output()) {
		s.terminal = failure(sdk.FailureInvalidArgument, "invalid generated message response target")
		return s.terminal
	}
	item, err := recv(s.ctx, s.responses, s.client.policy.GRPC.GeneratedClient.ResponseLimits, s.count, s.aggregate)
	if err != nil {
		if err == io.EOF {
			cursor := sdk.ResponseStreamMetadataOf(s.responses).Continuation
			if !validContinuation(cursor) {
				err = invalidResponse()
			} else {
				s.continuation = cursor
			}
		}
		s.terminal = err
		return err
	}
	if err := decode(s.ctx, item.Body, response); err != nil {
		s.terminal = err
		return err
	}
	s.count++
	s.aggregate += int64(len(item.Body))
	return nil
}

// Continuation returns the optional host-projected cursor only after clean EOF.
// It never exposes raw headers or trailers; an absent cursor returns "", false.
func (s *Stream) Continuation() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.continuation, s.terminal == io.EOF && s.continuation != ""
}

func validContinuation(cursor string) bool {
	if len(cursor) > 4096 {
		return false
	}
	for i := range cursor {
		if cursor[i] < 0x20 || cursor[i] > 0x7e {
			return false
		}
	}
	return true
}
