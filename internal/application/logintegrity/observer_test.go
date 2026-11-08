package logintegrity

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

type fixtureDigestServer struct {
	rafttransportpb.UnimplementedLogIntegrityServiceServer

	hash         []byte
	hashForRange func(uint64, uint64) []byte
	head         uint64
	applied      uint64
	dataLoss     bool
	empty        bool
}

func (s *fixtureDigestServer) GetRangeDigest(_ context.Context, req *rafttransportpb.GetRangeDigestRequest) (*rafttransportpb.GetRangeDigestResponse, error) {
	if s.dataLoss {
		return nil, status.Error(codes.DataLoss, "empty log")
	}
	if s.empty {
		return nil, status.Error(codes.NotFound, "log is empty")
	}
	hash := s.hash
	if s.hashForRange != nil {
		hash = s.hashForRange(req.GetFirstLogSequence(), req.GetLastLogSequence())
	}

	return &rafttransportpb.GetRangeDigestResponse{
		FirstLogSequence: req.GetFirstLogSequence(), LastLogSequence: req.GetLastLogSequence(),
		DigestVersion: DigestVersion, Digest: hash, AppliedIndex: func() uint64 {
			if s.applied != 0 {
				return s.applied
			}

			return req.GetMinAppliedIndex()
		}(),
		LastLogSequenceOnNode: func() uint64 {
			if s.head != 0 {
				return s.head
			}

			return req.GetLastLogSequence()
		}(),
	}, nil
}

func TestObserverKeepsEmptyProbeUntilFirstLog(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{empty: true}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///peer-empty-probe", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	local := &fixtureDigestServer{empty: true}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	for range 2 {
		observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 1, 5, 0)
		require.Equal(t, uint64(1), cursor.next)
		require.Empty(t, cursor.lastMismatch)
	}
	remote.empty = false
	local.empty = false
	local.hash = make([]byte, DigestSize)
	remote.hash = make([]byte, DigestSize)
	observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 1, 5, 1)
	require.Equal(t, uint64(2), cursor.next)
}

func TestObserverReportsPeerDataLossWithEmptyLocalLog(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{dataLoss: true}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///peer-corrupt-empty-local", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	local := &fixtureDigestServer{empty: true}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 1, 5, 0)
	require.NotEmpty(t, cursor.lastMismatch)
}

func TestObserverReportsPeerSuffixWithEmptyLocalLog(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{hash: make([]byte, DigestSize), head: 100, applied: 5}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///peer-empty-local", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{empty: true}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 1, 5, 0)
	require.Equal(t, uint64(2), cursor.next)
	require.NotEmpty(t, cursor.lastMismatch)
}

func TestObserverReportsPeerLogSuffix(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{hash: make([]byte, DigestSize), head: 100}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///peer-suffix", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{hash: make([]byte, DigestSize)}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 50, 5, 50)
	require.Equal(t, uint64(51), cursor.next)
	require.NotEmpty(t, cursor.lastMismatch)
}

func TestObserverDefersPeerSuffixAtNewerAppliedHorizon(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{hash: make([]byte, DigestSize), head: 100, applied: 6}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///peer-newer-horizon", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{hash: make([]byte, DigestSize)}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeerAtHead(t.Context(), 2, cursor, 1, 50, 5, 50)
	require.Equal(t, uint64(51), cursor.next)
	require.Empty(t, cursor.lastMismatch)
}

func fixtureRangeHash(values []byte) func(uint64, uint64) []byte {
	return func(first, last uint64) []byte {
		h := xxh3.New128()
		// hash.Hash.Write does not return an error.
		_, _ = h.Write(values[first-1 : last])

		return h.Sum(nil)
	}
}

type fixturePeerConnection struct{ conn *grpc.ClientConn }

func (p fixturePeerConnection) GetPeerConnection(uint64) *grpc.ClientConn { return p.conn }

func TestObserverComparesPeerDigestAndContinuesAfterMismatch(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{hash: make([]byte, DigestSize)}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///log-integrity", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{hash: make([]byte, DigestSize)}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeer(t.Context(), 2, cursor, 1, 1, 5)
	require.Equal(t, uint64(2), cursor.next)
	require.Empty(t, cursor.lastMismatch)

	remote.hash = make([]byte, DigestSize)
	remote.hash[0] = 1
	cursor.next = 1
	observer.comparePeer(t.Context(), 2, cursor, 1, 1, 5)
	require.Equal(t, uint64(2), cursor.next, "a divergent range must not block later logs")
	require.NotEmpty(t, cursor.lastMismatch)

	remote.hash[0] = 0
	maxSequence := ^uint64(0)
	observer.comparePeer(t.Context(), 2, cursor, maxSequence, maxSequence, 5)
	require.True(t, cursor.done, "the final uint64 sequence must not wrap to zero")
}

func TestObserverKeepsOldPeerQueuedWithoutCorruptionAlarm(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer() // Old binary: LogIntegrityService is absent.
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///old-peer", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{hash: make([]byte, DigestSize)}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	cursor := &peerCursor{next: 1}
	observer.comparePeer(t.Context(), 2, cursor, 1, 1, 5)
	require.Equal(t, uint64(1), cursor.next)
	require.Empty(t, cursor.lastMismatch)
}

func TestObserverLocatesFirstDifferingLog(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	remote := &fixtureDigestServer{hashForRange: fixtureRangeHash([]byte{1, 2, 3, 4, 9, 6, 7, 8})}
	server := grpc.NewServer()
	rafttransportpb.RegisterLogIntegrityServiceServer(server, remote)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bisect-peer", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	local := &fixtureDigestServer{hashForRange: fixtureRangeHash([]byte{1, 2, 3, 4, 5, 6, 7, 8})}
	observer := NewObserver(nil, nil, fixturePeerConnection{conn}, local,
		logging.FromContext(logging.TestingContext()), noop.NewMeterProvider().Meter("test"))
	sequence, err := observer.firstDifference(t.Context(), conn, 1, 8, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(5), sequence)
}
