//go:build e2e

package business

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	sharedClient        servicepb.BucketServiceClient
	sharedClusterClient clusterpb.ClusterServiceClient
	sharedCtx           context.Context
	sharedHTTPPort      int
)

// sharedNodePorts carries the shared node's allocated ports from process 1 to
// every other Ginkgo process. The ports are no longer constants, so they have
// to travel with the SynchronizedBeforeSuite payload.
type sharedNodePorts struct {
	HTTP int `json:"http"`
	GRPC int `json:"grpc"`
}

// SynchronizedBeforeSuite: process 1 starts the server, serializes port info.
// All processes deserialize and create their gRPC clients.
var _ = SynchronizedBeforeSuite(func() []byte {
	ctx, node := testutil.SetupSingleNode()
	sharedCtx = ctx
	sharedClient = node.Client
	sharedClusterClient = node.ClusterClient
	sharedHTTPPort = node.HTTPPort

	payload, err := json.Marshal(sharedNodePorts{
		HTTP: node.HTTPPort,
		GRPC: node.GRPCPort,
	})
	Expect(err).To(Succeed())

	return payload
}, func(data []byte) {
	var ports sharedNodePorts
	Expect(json.Unmarshal(data, &ports)).To(Succeed())

	client, clusterClient, conn, err := testutil.NewGRPCClient(ports.GRPC)
	Expect(err).To(Succeed())

	sharedClient = client
	sharedClusterClient = clusterClient
	sharedCtx = logging.TestingContext()
	sharedHTTPPort = ports.HTTP

	DeferCleanup(func() { _ = conn.Close() })
})

func TestBusiness(t *testing.T) {
	SetDefaultEventuallyPollingInterval(100 * time.Millisecond)
	SetDefaultEventuallyTimeout(5 * time.Second)
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Business Suite")
}
