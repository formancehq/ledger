package node

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

// fakeBidiStream is a minimal grpc.BidiStreamingServer used to drive
// DefaultTransport.StreamMessages through its metadata-validation prologue.
// Only Context() and Recv() are exercised: the fsm-determinism gate returns
// before any Send, and on the match path Recv() returns io.EOF to end the
// stream immediately.
type fakeBidiStream struct {
	ctx context.Context
}

func (f *fakeBidiStream) Context() context.Context { return f.ctx }
func (f *fakeBidiStream) Recv() (*rafttransportpb.SendMessageRequest, error) {
	return nil, io.EOF
}
func (f *fakeBidiStream) Send(*rafttransportpb.SendMessageResponse) error { return nil }
func (f *fakeBidiStream) SetHeader(metadata.MD) error                     { return nil }
func (f *fakeBidiStream) SendHeader(metadata.MD) error                    { return nil }
func (f *fakeBidiStream) SetTrailer(metadata.MD)                          {}
func (f *fakeBidiStream) SendMsg(any) error                               { return nil }
func (f *fakeBidiStream) RecvMsg(any) error                               { return io.EOF }

// streamWith builds an incoming-metadata context carrying the mandatory
// nodeID/priority keys plus whatever extra pairs the case needs.
func streamWith(extra map[string]string) *fakeBidiStream {
	md := metadata.New(map[string]string{
		MetadataKeyNodeID:   "2", // hex "2" == node 2
		MetadataKeyPriority: "high",
	})
	for k, v := range extra {
		md.Set(k, v)
	}

	return &fakeBidiStream{ctx: metadata.NewIncomingContext(context.Background(), md)}
}

func newTransportWithFlag(t *testing.T, flag bool) *DefaultTransport {
	t.Helper()

	return &DefaultTransport{
		logger:                logging.Testing(),
		fsmDeterminismEnabled: flag,
		peers:                 make(map[uint64]*peerConnection),
	}
}

// TestStreamMessages_RejectsMismatchedFSMDeterminism is the static-bootstrap
// regression: a peer whose fsm-determinism-enabled flag disagrees with this
// node's must have its Raft stream refused with FailedPrecondition. This is
// the enforcement point that covers seed nodes (which never call
// JoinAsLearner) — every peer establishes a Raft stream, so a divergent seed
// is caught here at connection time instead of desyncing the FSM digest.
func TestStreamMessages_RejectsMismatchedFSMDeterminism(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		localFlag bool
		peerMD    map[string]string
		wantCode  codes.Code
	}{
		{
			name:      "local ON, peer OFF",
			localFlag: true,
			peerMD:    map[string]string{MetadataKeyFSMDeterminism: "false"},
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "local OFF, peer ON",
			localFlag: false,
			peerMD:    map[string]string{MetadataKeyFSMDeterminism: "true"},
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "local ON, peer absent (old binary, treated as OFF)",
			localFlag: true,
			peerMD:    nil,
			wantCode:  codes.FailedPrecondition,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := newTransportWithFlag(t, tc.localFlag)
			err := tr.StreamMessages(streamWith(tc.peerMD))

			require.Error(t, err)
			require.Equal(t, tc.wantCode, status.Code(err))
			require.Contains(t, err.Error(), "fsm-determinism-enabled mismatch")
		})
	}
}

// TestStreamMessages_AcceptsMatchingFSMDeterminism confirms the gate is
// transparent when the flags agree: a matching peer proceeds past the check
// and the stream ends cleanly on the fake's io.EOF (no FailedPrecondition).
func TestStreamMessages_AcceptsMatchingFSMDeterminism(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		localFlag bool
		peerMD    map[string]string
	}{
		{
			name:      "both ON",
			localFlag: true,
			peerMD:    map[string]string{MetadataKeyFSMDeterminism: "true"},
		},
		{
			name:      "both OFF (explicit)",
			localFlag: false,
			peerMD:    map[string]string{MetadataKeyFSMDeterminism: "false"},
		},
		{
			name:      "both OFF (peer absent, old binary)",
			localFlag: false,
			peerMD:    nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr := newTransportWithFlag(t, tc.localFlag)
			err := tr.StreamMessages(streamWith(tc.peerMD))

			// The fake's Recv returns io.EOF once the prologue passes, so the
			// stream loop exits with that error — NOT a FailedPrecondition.
			require.ErrorIs(t, err, io.EOF)
		})
	}
}
