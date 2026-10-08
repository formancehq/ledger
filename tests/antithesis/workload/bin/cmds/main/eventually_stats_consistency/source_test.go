package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// The tests use the generated client over a real gRPC transport. In particular,
// trailers and errors after partial stream delivery follow the production wire
// contract instead of being supplied by a hand-written client mock.
type oracleTestServer struct {
	ledgerpb.UnimplementedBucketServiceServer

	barrierFn     func(context.Context, *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error)
	listLedgersFn func(*ledgerpb.ListLedgersRequest, ledgerpb.BucketService_ListLedgersServer) error
	listLogsFn    func(*ledgerpb.ListLogsRequest, ledgerpb.BucketService_ListLogsServer) error
	getLedgerFn   func(context.Context, *ledgerpb.GetLedgerRequest) (*ledgerpb.LedgerInfo, error)
	statsFn       func(context.Context, *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error)
	applyFn       func(context.Context, *ledgerpb.ApplyRequest) (*ledgerpb.ApplyResponse, error)
}

func (s *oracleTestServer) Barrier(ctx context.Context, req *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
	if s.barrierFn == nil {
		return s.UnimplementedBucketServiceServer.Barrier(ctx, req)
	}

	return s.barrierFn(ctx, req)
}

func (s *oracleTestServer) ListLedgers(req *ledgerpb.ListLedgersRequest, stream ledgerpb.BucketService_ListLedgersServer) error {
	if s.listLedgersFn == nil {
		return s.UnimplementedBucketServiceServer.ListLedgers(req, stream)
	}

	return s.listLedgersFn(req, stream)
}

func (s *oracleTestServer) ListLogs(req *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
	if s.listLogsFn == nil {
		return s.UnimplementedBucketServiceServer.ListLogs(req, stream)
	}

	return s.listLogsFn(req, stream)
}

func (s *oracleTestServer) GetLedger(ctx context.Context, req *ledgerpb.GetLedgerRequest) (*ledgerpb.LedgerInfo, error) {
	if s.getLedgerFn == nil {
		return s.UnimplementedBucketServiceServer.GetLedger(ctx, req)
	}

	return s.getLedgerFn(ctx, req)
}

func (s *oracleTestServer) GetLedgerStats(ctx context.Context, req *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error) {
	if s.statsFn == nil {
		return s.UnimplementedBucketServiceServer.GetLedgerStats(ctx, req)
	}

	return s.statsFn(ctx, req)
}

func (s *oracleTestServer) Apply(ctx context.Context, req *ledgerpb.ApplyRequest) (*ledgerpb.ApplyResponse, error) {
	if s.applyFn == nil {
		return s.UnimplementedBucketServiceServer.Apply(ctx, req)
	}

	return s.applyFn(ctx, req)
}

func newOracleTestConn(t *testing.T, bucket ledgerpb.BucketServiceServer, cluster ...ledgerpb.ClusterServiceServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	ledgerpb.RegisterBucketServiceServer(server, bucket)
	for _, clusterServer := range cluster {
		ledgerpb.RegisterClusterServiceServer(server, clusterServer)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close() // Stop may already have closed the listener.
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serving oracle fixture: %v", err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///oracle-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return conn
}

func newSourceTestClient(t *testing.T, server *oracleTestServer) ledgerpb.BucketServiceClient {
	t.Helper()

	return ledgerpb.NewBucketServiceClient(newOracleTestConn(t, server))
}

func sourceTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func sourceTestLog(ledger string, id uint64, data *ledgerpb.LedgerLogPayload) *ledgerpb.Log {
	return &ledgerpb.Log{
		// Deliberately unlike the ledger-local ID: pagination must not resume
		// from this global sequence.
		Sequence: 10_000 + id,
		Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{Apply: &ledgerpb.ApplyLedgerLog{
			LedgerName: ledger, Log: &ledgerpb.LedgerLog{Id: id, Data: data},
		}}},
	}
}

func sourceTestTransaction(id uint64, postingCount int) *ledgerpb.Transaction {
	postings := make([]*ledgerpb.Posting, postingCount)
	for i := range postings {
		postings[i] = &ledgerpb.Posting{Source: "world", Destination: fmt.Sprintf("account:%d", i), Asset: "USD", Amount: ledgerpb.NewUint256FromUint64(1)}
	}

	return &ledgerpb.Transaction{Id: id, Postings: postings}
}

func sourceTestCreatedLog(ledger string, id, txID uint64, postingCount int) *ledgerpb.Log {
	return sourceTestLog(ledger, id, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
		CreatedTransaction: &ledgerpb.CreatedTransaction{Transaction: sourceTestTransaction(txID, postingCount)},
	}})
}

func TestCaptureSourceDrainsPaginatedLedgersAndLogs(t *testing.T) {
	t.Parallel()
	const ledgerCount = 103
	const logCount = 205
	var barriers, ledgerPages, logPages atomic.Uint32
	server := &oracleTestServer{
		barrierFn: func(context.Context, *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
			return &ledgerpb.BarrierResponse{CommitIndex: 40 + uint64(barriers.Add(1))}, nil
		},
		listLedgersFn: func(req *ledgerpb.ListLedgersRequest, stream ledgerpb.BucketService_ListLedgersServer) error {
			ledgerPages.Add(1)
			if req.GetOptions().GetPageSize() != 100 {
				return status.Error(codes.InvalidArgument, "fixture requires 100-row pages")
			}
			start := 0
			switch req.GetOptions().GetCursor() {
			case "":
			case "ledger-099":
				start = 100
			default:
				return status.Errorf(codes.InvalidArgument, "unexpected ledger cursor %q", req.GetOptions().GetCursor())
			}
			end := min(start+100, ledgerCount)
			for i := start; i < end; i++ {
				if err := stream.Send(&ledgerpb.LedgerInfo{Name: fmt.Sprintf("ledger-%03d", i), Id: uint32(i + 1)}); err != nil {
					return err
				}
			}
			if end < ledgerCount {
				stream.SetTrailer(metadata.Pairs("x-next-cursor", "ledger-099"))
			}

			return nil
		},
		listLogsFn: func(req *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
			logPages.Add(1)
			if req.GetLedger() != "ledger-000" {
				return nil
			}
			start := uint64(0)
			if cursor := req.GetOptions().GetCursor(); cursor != "" {
				var err error
				start, err = strconv.ParseUint(cursor, 10, 64)
				if err != nil || (start != 100 && start != 200) {
					return status.Errorf(codes.InvalidArgument, "unexpected log cursor %q", cursor)
				}
			}
			end := min(start+100, uint64(logCount))
			for id := start + 1; id <= end; id++ {
				if err := stream.Send(sourceTestCreatedLog(req.GetLedger(), id, id*10, 2)); err != nil {
					return err
				}
			}
			if end < logCount {
				stream.SetTrailer(metadata.Pairs("x-next-cursor", strconv.FormatUint(end, 10)))
			}

			return nil
		},
		statsFn: func(_ context.Context, req *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error) {
			if req.GetLedger() == "ledger-000" {
				// Neither a tx ID boundary nor lagged/corrupt usage values may
				// influence the expected posting/revert fold.
				return &ledgerpb.LedgerStats{LogCount: logCount, TransactionCount: 50_000, PostingCount: 999, RevertCount: 999}, nil
			}

			return &ledgerpb.LedgerStats{}, nil
		},
	}
	got, horizon, err := captureSource(sourceTestContext(t), newSourceTestClient(t, server))
	require.NoError(t, err)
	require.Equal(t, uint64(42), horizon)
	require.Len(t, got, ledgerCount)
	require.Equal(t, expectedLedger{ID: 1, Counts: counts{Logs: logCount, Postings: 410}}, got["ledger-000"])
	require.Equal(t, expectedLedger{ID: 103}, got["ledger-102"])
	require.Equal(t, uint32(2), ledgerPages.Load())
	require.Equal(t, uint32(ledgerCount+2), logPages.Load())
	require.Equal(t, uint32(2), barriers.Load())
}

func TestCaptureSourceFoldsExactLogPayloads(t *testing.T) {
	t.Parallel()
	const ledger = "mirror"
	logs := []*ledgerpb.Log{
		sourceTestCreatedLog(ledger, 1, 42, 2),
		sourceTestLog(ledger, 2, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &ledgerpb.RevertedTransaction{
			RevertedTransactionId: 42, RevertTransaction: sourceTestTransaction(99, 3),
		}}}),
		// A mirror payload's actual postings are the expectation, even when
		// empty; transaction IDs are not a source cardinality.
		sourceTestCreatedLog(ledger, 3, 110, 0),
		sourceTestLog(ledger, 4, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &ledgerpb.SavedMetadata{}}}),
		sourceTestLog(ledger, 5, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &ledgerpb.OrderSkippedLog{}}}),
		sourceTestLog(ledger, 6, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_FillGap{FillGap: &ledgerpb.FilledGapLog{OriginalId: 999}}}),
	}
	server := sourceTestSingleLedger(ledger, logs, uint64(len(logs)))
	server.statsFn = func(context.Context, *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error) {
		return &ledgerpb.LedgerStats{LogCount: 6, TransactionCount: 1_000, PostingCount: 500, RevertCount: 500}, nil
	}
	got, _, err := captureSource(sourceTestContext(t), newSourceTestClient(t, server))
	require.NoError(t, err)
	require.Equal(t, expectedLedger{ID: 7, Counts: counts{Logs: 6, Postings: 5, Reverts: 1}}, got[ledger])
}

func sourceTestSingleLedger(name string, logs []*ledgerpb.Log, mainLogCount uint64) *oracleTestServer {
	var barriers atomic.Uint64

	return &oracleTestServer{
		barrierFn: func(context.Context, *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
			return &ledgerpb.BarrierResponse{CommitIndex: 10 + barriers.Add(1)}, nil
		},
		listLedgersFn: func(_ *ledgerpb.ListLedgersRequest, stream ledgerpb.BucketService_ListLedgersServer) error {
			return stream.Send(&ledgerpb.LedgerInfo{Name: name, Id: 7})
		},
		listLogsFn: func(_ *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
			for _, entry := range logs {
				if err := stream.Send(entry); err != nil {
					return err
				}
			}

			return nil
		},
		statsFn: func(context.Context, *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error) {
			return &ledgerpb.LedgerStats{LogCount: mainLogCount}, nil
		},
	}
}

func TestCaptureSourceRejectsIncompleteLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ids  []uint64
		want string
	}{
		{name: "missing middle log", ids: []uint64{1, 3}, want: "non-contiguous or foreign log after 1"},
		{name: "early clean EOF", ids: []uint64{1}, want: "incomplete source for ledger"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logs []*ledgerpb.Log
			for _, id := range tc.ids {
				logs = append(logs, sourceTestCreatedLog("ledger", id, id, 1))
			}
			got, horizon, err := captureSource(sourceTestContext(t), newSourceTestClient(t, sourceTestSingleLedger("ledger", logs, 3)))
			require.ErrorContains(t, err, tc.want)
			require.False(t, isSourceChanged(err))
			require.Nil(t, got, "partial source must never be published")
			require.Zero(t, horizon)
		})
	}
}

func TestCaptureSourceRejectsRepeatedCursors(t *testing.T) {
	t.Parallel()
	for _, collection := range []string{"ledgers", "logs"} {
		t.Run(collection, func(t *testing.T) {
			t.Parallel()
			server := sourceTestSingleLedger("ledger", nil, 2)
			var pages atomic.Uint64
			if collection == "ledgers" {
				server.listLedgersFn = func(_ *ledgerpb.ListLedgersRequest, stream ledgerpb.BucketService_ListLedgersServer) error {
					page := pages.Add(1)
					if err := stream.Send(&ledgerpb.LedgerInfo{Name: fmt.Sprintf("ledger-%d", page), Id: uint32(page)}); err != nil {
						return err
					}
					stream.SetTrailer(metadata.Pairs("x-next-cursor", "ledger-1"))

					return nil
				}
			} else {
				server.listLogsFn = func(_ *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
					page := pages.Add(1)
					if err := stream.Send(sourceTestCreatedLog("ledger", page, page, 1)); err != nil {
						return err
					}
					stream.SetTrailer(metadata.Pairs("x-next-cursor", "1"))

					return nil
				}
			}
			got, horizon, err := captureSource(sourceTestContext(t), newSourceTestClient(t, server))
			require.ErrorContains(t, err, "invalid or repeated pagination cursor")
			require.Nil(t, got)
			require.Zero(t, horizon)
			require.Equal(t, uint64(2), pages.Load(), "repeated token must terminate pagination")
		})
	}
}

func TestCaptureSourceDiscardsLaterPageStreamError(t *testing.T) {
	t.Parallel()
	server := sourceTestSingleLedger("ledger", nil, 101)
	var pages, statsCalls atomic.Uint32
	server.listLogsFn = func(req *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
		pages.Add(1)
		if req.GetOptions().GetCursor() == "" {
			for id := uint64(1); id <= 100; id++ {
				if err := stream.Send(sourceTestCreatedLog("ledger", id, id, 1)); err != nil {
					return err
				}
			}
			stream.SetTrailer(metadata.Pairs("x-next-cursor", "100"))

			return nil
		}
		if req.GetOptions().GetCursor() != "100" {
			return status.Error(codes.InvalidArgument, "incorrect continuation token")
		}
		if err := stream.Send(sourceTestCreatedLog("ledger", 101, 101, 1)); err != nil {
			return err
		}

		return status.Error(codes.Internal, "injected later-page storage failure")
	}
	server.statsFn = func(context.Context, *ledgerpb.GetLedgerStatsRequest) (*ledgerpb.LedgerStats, error) {
		statsCalls.Add(1)

		return &ledgerpb.LedgerStats{LogCount: 101}, nil
	}
	got, horizon, err := captureSource(sourceTestContext(t), newSourceTestClient(t, server))
	require.ErrorContains(t, err, "injected later-page storage failure")
	require.Equal(t, codes.Internal, status.Code(err))
	require.Nil(t, got, "receiving every row before an error is still not clean completion")
	require.Zero(t, horizon)
	require.Equal(t, uint32(2), pages.Load())
	require.Zero(t, statsCalls.Load())
}

func TestCaptureSourceChangedHorizonInvalidatesReadVerdict(t *testing.T) {
	t.Parallel()
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%t", malformed), func(t *testing.T) {
			t.Parallel()
			id := uint64(1)
			if malformed {
				id = 2 // Could be a deletion/recreation racing the capture.
			}
			server := sourceTestSingleLedger("ledger", []*ledgerpb.Log{sourceTestCreatedLog("ledger", id, 1, 1)}, 1)
			var barriers atomic.Uint64
			server.barrierFn = func(context.Context, *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
				return &ledgerpb.BarrierResponse{CommitIndex: 10 + 2*barriers.Add(1)}, nil
			}
			got, horizon, err := captureSource(sourceTestContext(t), newSourceTestClient(t, server))
			require.ErrorIs(t, err, errSourceChanged)
			require.ErrorContains(t, err, "during capture: 12 -> 14")
			require.NotContains(t, err.Error(), "non-contiguous", "an invalid horizon cannot publish the captured source error")
			require.Nil(t, got)
			require.Zero(t, horizon)
			require.Equal(t, uint64(2), barriers.Load(), "the source fence must run even after a read error")
		})
	}
}

func TestFoldLogsCancelsStreamAfterInvalidRow(t *testing.T) {
	t.Parallel()
	streamReleased := make(chan struct{})
	server := &oracleTestServer{
		listLogsFn: func(_ *ledgerpb.ListLogsRequest, stream ledgerpb.BucketService_ListLogsServer) error {
			defer close(streamReleased)
			if err := stream.Send(sourceTestCreatedLog("ledger", 2, 1, 1)); err != nil {
				return err
			}
			// A streaming server may retain its snapshot until the client
			// cancels. The invalid row must not leave that stream open.
			<-stream.Context().Done()

			return stream.Context().Err()
		},
	}
	ctx := sourceTestContext(t)
	_, err := foldLogs(ctx, newSourceTestClient(t, server), "ledger")
	require.ErrorContains(t, err, "non-contiguous or foreign log after 0")
	select {
	case <-streamReleased:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid log did not cancel its source stream")
	}
	require.NoError(t, ctx.Err(), "the page must close before the overall context expires")
}

func TestWriteWitnessAccountsForExactlyOneProposalAndBarrier(t *testing.T) {
	t.Parallel()
	for _, after := range []uint64{52, 53} {
		t.Run(strconv.FormatUint(after, 10), func(t *testing.T) {
			t.Parallel()
			applied := make(chan *ledgerpb.ApplyRequest, 1)
			server := &oracleTestServer{
				applyFn: func(_ context.Context, req *ledgerpb.ApplyRequest) (*ledgerpb.ApplyResponse, error) {
					applied <- req

					return &ledgerpb.ApplyResponse{}, nil
				},
				barrierFn: func(context.Context, *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
					return &ledgerpb.BarrierResponse{CommitIndex: after}, nil
				},
			}
			got, err := writeWitness(sourceTestContext(t), newSourceTestClient(t, server), "fresh-witness", 50)
			if after == 52 {
				require.NoError(t, err)
				require.Equal(t, after, got)
			} else {
				require.ErrorIs(t, err, errSourceChanged)
				require.ErrorContains(t, err, "during witness: 50 -> 53")
				require.Zero(t, got)
			}
			req := <-applied
			batch := req.GetUnsigned()
			require.Equal(t, "fresh-witness", batch.GetIdempotencyKey())
			require.Len(t, batch.GetRequests(), 1)
			apply := batch.GetRequests()[0].GetApply()
			require.Equal(t, "fresh-witness", apply.GetLedger())
			tx := apply.GetAction().GetCreateTransaction()
			require.Equal(t, "fresh-witness", tx.GetReference())
			require.Len(t, tx.GetPostings(), 1)
		})
	}
}
