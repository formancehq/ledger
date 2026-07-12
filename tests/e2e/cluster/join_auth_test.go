//go:build e2e

package cluster

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// EN-1080: a node started with --join against a cluster whose inter-node
// RaftServer requires a --cluster-secret must fail fast with a clear,
// actionable error when its own secret is missing or wrong. The join flow
// carries no user identity, so the only credential in play is the shared
// cluster secret; retrying with a bad one can only loop forever.
//
// Before EN-1080 the join RPC leaked the opaque gRPC status
// ("missing authorization metadata on Raft RPC" / "invalid cluster
// credentials on Raft RPC") up the bootstrap chain. Now the joining node's
// startup returns a JoinAuthError telling the operator exactly which lever
// to pull.
var _ = Describe("Cluster join with cluster-secret required", Ordered, func() {
	var (
		certs         *testserver.TestCerts
		bootstrapRaft int
		clusterSecret = "correct-cluster-secret-en1080"
		clusterID     = "en1080-cluster"
	)

	// startJoiner builds a joining node targeting the bootstrap node's raft
	// port with TLS enabled and the given (possibly empty/wrong) secret, then
	// returns the error from Start — nil means the node started (join
	// succeeded or is still pending).
	startJoiner := func(secretInstrument testservice.Instrumentation) error {
		ctx := logging.TestingContext()

		walDir := GinkgoT().TempDir()
		dataDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			_ = os.RemoveAll(walDir)
			_ = os.RemoveAll(dataDir)
		})

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    2,
			ClusterID: clusterID,
			HTTPPort:  freeTLSReproPort(),
			RaftPort:  freeTLSReproPort(),
			GRPCPort:  freeTLSReproPort(),
			WalDir:    walDir,
			DataDir:   dataDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithJoin(fmt.Sprintf("127.0.0.1:%d", bootstrapRaft)),
			testserver.WithTLSMode("required"),
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
			testserver.WithTLSCACertFile(certs.CACertFile),
		)
		if secretInstrument != nil {
			instruments = append(instruments, secretInstrument)
		}

		joiner := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		startErr := joiner.Start(ctx)
		DeferCleanup(func() {
			_ = joiner.Stop(ctx)
		})

		return startErr
	}

	BeforeAll(func() {
		ctx := logging.TestingContext()
		certDir := GinkgoT().TempDir()
		var err error
		certs, err = testserver.GenerateTestCerts(certDir)
		Expect(err).To(Succeed())

		walDir := GinkgoT().TempDir()
		dataDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			_ = os.RemoveAll(walDir)
			_ = os.RemoveAll(dataDir)
		})

		bootstrapRaft = freeTLSReproPort()

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: clusterID,
			HTTPPort:  freeTLSReproPort(),
			RaftPort:  bootstrapRaft,
			GRPCPort:  freeTLSReproPort(),
			WalDir:    walDir,
			DataDir:   dataDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithTLSMode("required"),
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
			testserver.WithTLSCACertFile(certs.CACertFile),
			testserver.WithClusterSecret(clusterSecret),
		)

		// The bootstrap node's RaftServer auth interceptor rejects a bad
		// Bearer before any membership/leadership logic runs, so a
		// missing/wrong secret always yields codes.Unauthenticated
		// regardless of election state. Starting the node is enough — no
		// leadership poll needed.
		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			_ = server.Stop(ctx)
		})
	})

	It("fails fast with an actionable error when the joining node has NO cluster-secret", func() {
		err := startJoiner(nil)
		Expect(err).To(HaveOccurred(),
			"join must not succeed against a secret-protected cluster without a secret")
		Expect(err.Error()).To(ContainSubstring("inter-node authentication failed"))
		Expect(err.Error()).To(ContainSubstring("set --cluster-secret"))
	})

	It("fails fast with an actionable error when the joining node has a WRONG cluster-secret", func() {
		err := startJoiner(testserver.WithClusterSecret("wrong-secret-value-xyz"))
		Expect(err).To(HaveOccurred(),
			"join must not succeed against a secret-protected cluster with a mismatched secret")
		Expect(err.Error()).To(ContainSubstring("inter-node authentication failed"))
		Expect(err.Error()).To(ContainSubstring("verify the secret matches"))
	})
})
