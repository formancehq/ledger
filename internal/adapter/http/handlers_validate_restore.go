package http

import (
	"context"
	"errors"
	"io"
	"net/http"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

// validateRestoreResult is the last line of a validation stream.
type validateRestoreResult struct {
	Passed       bool   `json:"passed"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// handleValidateRestore handles POST /_/restore/validate. The response is
// NDJSON: one ValidateRestoreEvent per line, then {"result": ...}. A failure
// before the first event is an ordinary error response; a later one is
// reported in the result line, since the status is already sent.
func (s *restoreServer) handleValidateRestore(w http.ResponseWriter, r *http.Request) {
	stream := &validateRestoreStream{w: w, r: r}
	err := s.backend.ValidateRestore(&restorepb.ValidateRestoreRequest{}, stream)
	if !stream.started {
		if err != nil {
			handleClusterError(w, r, err)

			return
		}
		stream.start()
	}

	result := validateRestoreResult{Passed: err == nil && stream.findings == 0}
	if err != nil {
		c, normalized := classifyClusterError(err)
		result.ErrorCode, result.ErrorMessage = c.code, c.publicMessage(r, normalized)
	}
	// The client may be gone; there is no one left to report a write error to.
	_ = stream.writeLine(struct {
		Result validateRestoreResult `json:"result"`
	}{result})
}

// validateRestoreStream adapts the RestoreService validation stream to NDJSON.
type validateRestoreStream struct {
	w        http.ResponseWriter
	r        *http.Request
	started  bool
	findings int
}

var _ ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent] = (*validateRestoreStream)(nil)

func (s *validateRestoreStream) start() {
	s.started = true
	s.w.Header().Set("Content-Type", "application/x-ndjson")
	s.w.WriteHeader(http.StatusOK)
}

func (s *validateRestoreStream) writeLine(v any) error {
	if err := json.MarshalWriteWithOptions(s.w, v, bigIntegerEncoding(s.r)); err != nil {
		return err
	}
	if err := http.NewResponseController(s.w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}

	return nil
}

func (s *validateRestoreStream) Send(event *restorepb.ValidateRestoreEvent) error {
	if !s.started {
		s.start()
	}
	if event.GetError() != nil {
		s.findings++
	}

	return s.writeLine(event)
}

func (s *validateRestoreStream) Context() context.Context { return s.r.Context() }

func (s *validateRestoreStream) SendMsg(m any) error {
	event, ok := m.(*restorepb.ValidateRestoreEvent)
	if !ok {
		return errors.New("unexpected validation stream message")
	}

	return s.Send(event)
}

func (s *validateRestoreStream) RecvMsg(any) error            { return io.EOF }
func (s *validateRestoreStream) SetHeader(metadata.MD) error  { return nil }
func (s *validateRestoreStream) SendHeader(metadata.MD) error { return nil }
func (s *validateRestoreStream) SetTrailer(metadata.MD)       {}
