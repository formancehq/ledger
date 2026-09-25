package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/adapter/auth"
	ledgergrpc "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise the production list handler and real gRPC trailers, rather than
// reproducing the pagination algorithm in a fake stream.
func TestNumscriptIsListedBeyondDefaultPage(t *testing.T) {
	t.Parallel()

	controller := ctrlmock.NewMockController(gomock.NewController(t))
	scripts := numscriptNames(105)
	controller.EXPECT().ListNumscripts(gomock.Any(), "default").DoAndReturn(
		func(context.Context, string) ([]*commonpb.NumscriptInfo, error) {
			return append([]*commonpb.NumscriptInfo(nil), scripts...), nil
		},
	).AnyTimes()

	client := dialNumscriptServer(t, numscriptServer(controller))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Control: the service returns exactly the default 100 items and provides
	// the continuation token that the old workload discarded.
	stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{Ledger: "default"})
	require.NoError(t, err)
	var count int
	for {
		_, err := stream.Recv()
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
		count++
	}
	require.Equal(t, 100, count)
	require.Equal(t, []string{"lifecycle-100"}, stream.Trailer().Get("x-next-cursor"))

	found, err := numscriptIsListed(ctx, client, "default", "lifecycle-102")
	require.NoError(t, err)
	require.True(t, found, "saved numscript should appear in ListNumscripts: rank 102 of 105, default page 100")
}

func dialNumscriptServer(t *testing.T, service servicepb.BucketServiceServer, options ...grpc.ServerOption) servicepb.BucketServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(options...)
	servicepb.RegisterBucketServiceServer(server, service)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveDone; err != nil {
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		}
	})
	conn, err := grpc.NewClient("passthrough:///numscript-pagination",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return servicepb.NewBucketServiceClient(conn)
}

func TestNumscriptIsListedPagination(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		count  int
		target string
		found  bool
		pages  int
	}{
		{name: "empty", pages: 1, target: "missing"},
		{name: "first page", count: 105, target: "lifecycle-001", found: true, pages: 2},
		{name: "last item", count: 105, target: "lifecycle-105", found: true, pages: 2},
		{name: "full final page", count: 200, target: "lifecycle-200", found: true, pages: 2},
		{name: "third page", count: 205, target: "lifecycle-205", found: true, pages: 3},
		{name: "absent after all pages", count: 105, target: "missing", pages: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := ctrlmock.NewMockController(gomock.NewController(t))
			controller.EXPECT().ListNumscripts(gomock.Any(), "default").DoAndReturn(
				func(context.Context, string) ([]*commonpb.NumscriptInfo, error) {
					return numscriptNames(test.count), nil
				},
			).Times(test.pages)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			found, err := numscriptIsListed(ctx, dialNumscriptServer(t, numscriptServer(controller)), "default", test.target)
			require.NoError(t, err)
			require.Equal(t, test.found, found)
		})
	}
}

func TestNumscriptIsListedErrors(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		page   int
		target string
	}{
		{name: "first page fails", page: 1, target: "lifecycle-102"},
		{name: "second page fails", page: 2, target: "lifecycle-102"},
		{name: "found before second page fails", page: 2, target: "lifecycle-001"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := ctrlmock.NewMockController(gomock.NewController(t))
			calls := make([]any, 0, test.page)
			if test.page == 2 {
				calls = append(calls, controller.EXPECT().ListNumscripts(gomock.Any(), "default").Return(numscriptNames(105), nil))
			}
			calls = append(calls, controller.EXPECT().ListNumscripts(gomock.Any(), "default").Return(nil, status.Error(codes.InvalidArgument, "injected list failure")))
			gomock.InOrder(calls...)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			found, err := numscriptIsListed(ctx, dialNumscriptServer(t, numscriptServer(controller)), "default", test.target)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, "injected list failure")
			require.False(t, found, "an incomplete list is never a successful presence check")
		})
	}
}

func TestNumscriptIsListedReceiveErrorAfterMatch(t *testing.T) {
	t.Parallel()
	controller := ctrlmock.NewMockController(gomock.NewController(t))
	controller.EXPECT().ListNumscripts(gomock.Any(), "default").Return(numscriptNames(105), nil).Times(2)
	client := dialNumscriptServer(t, numscriptServer(controller), grpc.StreamInterceptor(
		func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, &failingNumscriptStream{ServerStream: stream})
		}))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	found, err := numscriptIsListed(ctx, client, "default", "lifecycle-102")
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.ErrorContains(t, err, "injected mid-stream failure")
	require.False(t, found)
}

// Fail the second page after sending the searched-for name over real gRPC.
type failingNumscriptStream struct {
	grpc.ServerStream
}

func (s *failingNumscriptStream) SendMsg(msg any) error {
	if err := s.ServerStream.SendMsg(msg); err != nil {
		return err
	}
	if msg.(*commonpb.NumscriptInfo).GetName() == "lifecycle-102" {
		return status.Error(codes.DataLoss, "injected mid-stream failure")
	}
	return nil
}

func TestNumscriptIsListedCanceled(t *testing.T) {
	t.Parallel()
	controller := ctrlmock.NewMockController(gomock.NewController(t))
	client := dialNumscriptServer(t, numscriptServer(controller))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	found, err := numscriptIsListed(ctx, client, "default", "missing")
	require.Equal(t, codes.Canceled, status.Code(err))
	require.False(t, found)
}

func numscriptNames(count int) []*commonpb.NumscriptInfo {
	scripts := make([]*commonpb.NumscriptInfo, count)
	for i := range scripts {
		// Return unsorted data so the production handler also exercises sorting.
		scripts[i] = &commonpb.NumscriptInfo{Name: fmt.Sprintf("lifecycle-%03d", count-i), Version: "1.0.0"}
	}
	return scripts
}

func numscriptServer(controller *ctrlmock.MockController) servicepb.BucketServiceServer {
	return ledgergrpc.NewBucketServiceServer(logging.Testing(), controller, nil, nil, nil, nil, nil, nil,
		auth.AuthConfig{}, 0, "", noop.NewMeterProvider(), nil, nil, version.Info{})
}
