//go:build e2e

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	jose "github.com/go-jose/go-jose/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type synchronizedOutput struct {
	mu sync.Mutex
	b  strings.Builder
}

func (o *synchronizedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.Write(p)
}

func (o *synchronizedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.String()
}

// Only the OIDC HTTP response is perturbed. WAL, Pebble, PVCs and Raft traffic
// are never edited by the test. The same node dirs are reused after startup
// fails, as they would be after a container restart.
var _ = Describe("Joiner OIDC network failure before restart", func() {
	It("retains outbound Raft routing after the OIDC endpoint recovers", func() {
		ctx := logging.TestingContext()
		certs, err := testserver.GenerateTestCerts(GinkgoT().TempDir())
		Expect(err).To(Succeed())

		key, err := rsa.GenerateKey(rand.Reader, 2048)
		Expect(err).To(Succeed())
		jwks := map[string]any{"keys": []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "network-test", Algorithm: string(jose.RS256), Use: "sig"}}}
		var oidcAvailable atomic.Bool
		var issuer *httptest.Server
		issuer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !oidcAvailable.Load() {
				http.Error(w, "temporary network failure", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/.well-known/openid-configuration":
				_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer.URL, "jwks_uri": issuer.URL + "/jwks"})
			case "/jwks":
				_ = json.NewEncoder(w).Encode(jwks)
			default:
				http.NotFound(w, r)
			}
		}))
		DeferCleanup(issuer.Close)

		leaderLease := testserver.AllocateNodeLease()
		leaderWAL, leaderData := GinkgoT().TempDir(), GinkgoT().TempDir()
		joinerWAL, joinerData := GinkgoT().TempDir(), GinkgoT().TempDir()
		out := &synchronizedOutput{}

		common := func(id int, lease *testserver.NodeLease, walDir, dataDir string) []testservice.Instrumentation {
			return append(testserver.DefaultTestInstruments(testserver.TestNodeConfig{
				NodeID: id, ClusterID: "oidc-network-restart", Ports: lease.Ports(),
				WalDir: walDir, DataDir: dataDir, Output: out, TickInterval: 100 * time.Millisecond,
			}),
				testserver.WithTLSMode("required"),
				testserver.WithTLSCertFile(certs.ServerCertFile),
				testserver.WithTLSKeyFile(certs.ServerKeyFile),
				testserver.WithTLSCACertFile(certs.CACertFile),
			)
		}

		leaderInstruments := append(common(1, leaderLease, leaderWAL, leaderData), testserver.WithBootstrap())
		leader := leaderLease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(leaderInstruments...))
		Expect(leader.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = leader.Stop(stopCtx)
		})
		_, leaderClient, conn, err := newTLSGRPCClient(leaderLease.Ports().GRPC(), certs.CACertFile)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = conn.Close() })
		Eventually(func(g Gomega) {
			state, err := leaderClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			g.Expect(state.GetLeader()).To(Equal(uint64(1)))
		}).Within(10 * time.Second).Should(Succeed())

		// Run the joiner in another process: a failed Fx startup in the test
		// process keeps its Pebble lock and listeners open until process exit.
		binary := filepath.Join(GinkgoT().TempDir(), "ledger")
		build := exec.Command("go", "build", "-o", binary, ".")
		build.Dir = filepath.Clean("../../..")
		buildOutput, err := build.CombinedOutput()
		Expect(err).To(Succeed(), string(buildOutput))

		httpAddr := testserver.AllocateDeadAddress()
		raftAddr := testserver.AllocateDeadAddress()
		grpcAddr := testserver.AllocateDeadAddress()
		_, httpPort, err := net.SplitHostPort(httpAddr)
		Expect(err).To(Succeed())
		_, raftPort, err := net.SplitHostPort(raftAddr)
		Expect(err).To(Succeed())
		_, grpcPort, err := net.SplitHostPort(grpcAddr)
		Expect(err).To(Succeed())
		joinerArgs := []string{
			"run", "--node-id", "2", "--cluster-id", "oidc-network-restart",
			"--http-port", httpPort, "--wal-dir", joinerWAL, "--data-dir", joinerData,
			"--bind-addr", ":" + raftPort, "--grpc-port", grpcPort,
			"--maintenance-interval", "5s", "--raft-tick-interval", "100ms",
			"--raft-heartbeat-tick", "1", "--raft-election-tick", "10",
			"--health-wal-threshold", "1", "--health-data-threshold", "1",
			"--health-wal-resume-threshold", "0.99", "--health-data-resume-threshold", "0.99",
			"--tls-mode", "required", "--tls-cert-file", certs.ServerCertFile,
			"--tls-key-file", certs.ServerKeyFile, "--tls-ca-cert-file", certs.CACertFile,
			"--join", fmt.Sprintf("127.0.0.1:%d", leaderLease.Ports().Raft()),
			"--auth-enabled", "--auth-issuer", issuer.URL, "--auth-service", "ledger",
		}
		first := exec.Command(binary, joinerArgs...)
		firstOutput, firstErr := first.CombinedOutput()
		Expect(firstErr).To(HaveOccurred(), string(firstOutput))
		Expect(string(firstOutput)).To(ContainSubstring("Rehydrated cluster membership from Pebble"))
		Expect(string(firstOutput)).To(ContainSubstring("discovering OIDC configuration"))

		// Restore the only faulted network endpoint and restart the same node
		// against the same WAL and Pebble directories.
		oidcAvailable.Store(true)
		secondOutput := &synchronizedOutput{}
		second := exec.Command(binary, joinerArgs...)
		second.Stdout, second.Stderr = secondOutput, secondOutput
		Expect(second.Start()).To(Succeed())
		secondDone := make(chan error, 1)
		go func() { secondDone <- second.Wait() }()
		DeferCleanup(func() {
			_ = second.Process.Kill()
			<-secondDone
		})
		Eventually(func() bool {
			conn, err := net.DialTimeout("tcp", httpAddr, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				return true
			}
			return false
		}).Within(15*time.Second).Should(BeTrue(), secondOutput.String())
		Expect(secondOutput.String()).To(ContainSubstring("Restart detected: WAL already has ConfState"))
		Eventually(secondOutput.String).Within(5 * time.Second).
			Should(ContainSubstring("Peer 1 connected on high priority stream"))

		// A missing outbound peer route emits this on every Raft tick. A healthy
		// restarted learner must neither drop messages nor rely on its peers'
		// inbound streams to discover the missing address.
		Consistently(secondOutput.String).Within(3 * time.Second).ProbeEvery(100 * time.Millisecond).
			ShouldNot(ContainSubstring("No send channel for peer, dropping messages"))
	})
})
