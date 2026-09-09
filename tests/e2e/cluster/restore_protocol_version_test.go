//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var _ = Describe("Restore gRPC protocol version", func() {
	It("rejects old unary and streaming clients in restore mode while accepting the supported protocol", func() {
		ctx, cancel := context.WithTimeout(logging.TestingContext(), 30*time.Second)
		defer cancel()
		lease := testserver.AllocateNodeLease()
		ports := lease.Ports()
		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			Ports:     ports,
			WalDir:    GinkgoT().TempDir(),
			DataDir:   GinkgoT().TempDir(),
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments, testserver.WithRestore())
		server := lease.NewService(cmdserver.NewRunCommandWithBindings,
			testservice.WithInstruments(instruments...))
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		// No discovery handshake or protocol metadata, as with an old ledgerctl.
		rawConn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", ports.GRPC()),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(rawConn.Close()).To(Succeed()) })
		rawClient := restorepb.NewRestoreServiceClient(rawConn)

		_, err = grpc_health_v1.NewHealthClient(rawConn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		Expect(err).To(Succeed())
		_, err = rawClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{})
		expectProtocolRejection(err)
		stream, err := rawClient.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
		if err == nil {
			_, err = stream.Recv()
		}
		expectProtocolRejection(err)

		// The same lookup must reach restore business validation with the current
		// client; no object store or populated backup is needed to prove routing.
		req := &restorepb.GetDownloadStatusRequest{JobId: "not-started"}
		_, err = rawClient.GetDownloadStatus(ctx, req)
		expectProtocolRejection(err)
		_, _, matchingConn, err := testutil.NewGRPCClient(ports.GRPC())
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(matchingConn.Close()).To(Succeed()) })
		_, err = restorepb.NewRestoreServiceClient(matchingConn).GetDownloadStatus(ctx, req)
		Expect(status.Code(err)).To(Equal(codes.NotFound))
		Expect(status.Convert(err).Message()).To(Equal("unknown job id"))
	})
})
