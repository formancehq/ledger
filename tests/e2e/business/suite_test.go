//go:build e2e

package business

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	sharedClient        servicepb.BucketServiceClient
	sharedClusterClient clusterpb.ClusterServiceClient
	sharedCtx           context.Context
)

const signingKey = "test-receipt-signing-key-32bytes!"

// SynchronizedBeforeSuite: process 1 starts the server, serializes port info.
// All processes deserialize and create their gRPC clients.
var _ = SynchronizedBeforeSuite(func() []byte {
	// Process 1: start server, return serialized port info
	ctx, client, clusterClient := testutil.SetupSingleNode(
		testutil.TestSingleHTTPPort,
		testutil.TestSingleGRPCPort,
		testserver.WithReceiptSigningKey(signingKey),
	)
	sharedCtx = ctx
	sharedClient = client
	sharedClusterClient = clusterClient
	// Serialize gRPC port for other processes
	return []byte(fmt.Sprintf("%d", testutil.TestSingleGRPCPort))
}, func(data []byte) {
	// All processes: create gRPC client from serialized port
	port, _ := strconv.Atoi(string(data))
	client, clusterClient, conn, _ := testutil.NewGRPCClient(port)
	sharedClient = client
	sharedClusterClient = clusterClient
	sharedCtx = logging.TestingContext()
	DeferCleanup(func() { _ = conn.Close() })
})

func TestBusiness(t *testing.T) {
	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultEventuallyTimeout(5 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Business Suite")
}
