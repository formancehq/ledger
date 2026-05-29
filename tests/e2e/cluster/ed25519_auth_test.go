//go:build e2e

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	jose "github.com/go-jose/go-jose/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Dedicated port range for Ed25519 auth tests (separate from OIDC auth tests).
const (
	ed25519AuthTestHTTPPort = 15810
	ed25519AuthTestGRPCPort = 15910
	ed25519AuthTestRaftPort = 14810
)

// signEdDSAJWT creates a signed EdDSA JWT token with the given claims.
func signEdDSAJWT(privKey ed25519.PrivateKey, keyID string, claims *oidc.AccessTokenClaims) (string, error) {
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.EdDSA,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: keyID},
	}, nil)
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	jws, err := signer.Sign(payload)
	if err != nil {
		return "", err
	}

	return jws.CompactSerialize()
}

// makeEdDSAClaims creates AccessTokenClaims for EdDSA tokens (no issuer).
func makeEdDSAClaims(scopes ...string) *oidc.AccessTokenClaims {
	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = "ed25519-test-user"
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(1 * time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)
	return claims
}

// writeEd25519KeysConfig generates an Ed25519 keypair and writes the auth-keys.json config file.
// Returns the private key, key ID, and config file path.
func writeEd25519KeysConfig(dir string, keyID string, scopes []string) (ed25519.PrivateKey, string, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", err
	}
	pub := priv.Public().(ed25519.PublicKey)

	// Write public key as hex
	pubKeyFile := filepath.Join(dir, "pubkey.hex")
	if err := os.WriteFile(pubKeyFile, []byte(fmt.Sprintf("%x\n", pub)), 0644); err != nil {
		return nil, "", err
	}

	// Write config
	cfg := internalauth.Ed25519KeysConfig{
		Keys: []internalauth.Ed25519KeyEntry{
			{
				KeyID:         keyID,
				PublicKeyFile: pubKeyFile,
				Scopes:        scopes,
			},
		},
	}
	configData, err := json.Marshal(cfg)
	if err != nil {
		return nil, "", err
	}

	configPath := filepath.Join(dir, "auth-keys.json")
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return nil, "", err
	}

	return priv, configPath, nil
}

var _ = Describe("Ed25519 Auth", Ordered, func() {
	var (
		ctx           context.Context
		grpcConn      *grpc.ClientConn
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		edPrivKey     ed25519.PrivateKey
		keyID         string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		keyID = "e2e-test-key"
		keysDir := GinkgoT().TempDir()

		// Generate Ed25519 keypair and config file
		var configPath string
		var err error
		edPrivKey, configPath, err = writeEd25519KeysConfig(
			keysDir, keyID, []string{"ledger:read", "ledger:write", "ledger:admin"},
		)
		Expect(err).To(Succeed())

		// Start ledger server with Ed25519 auth (auto-enables auth + scope checking)
		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			Expect(os.RemoveAll(walTmpDir)).To(Succeed())
			Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
		})

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			HTTPPort:  ed25519AuthTestHTTPPort,
			RaftPort:  ed25519AuthTestRaftPort,
			GRPCPort:  ed25519AuthTestGRPCPort,
			WalDir:    walTmpDir,
			DataDir:   dataTmpDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithAuthEnabled(),
			testserver.WithAuthEd25519Keys(configPath),
			testserver.WithAuthService("ledger"),
		)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		// Create gRPC client
		grpcConn, err = grpc.NewClient(
			fmt.Sprintf("localhost:%d", ed25519AuthTestGRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		client = servicepb.NewBucketServiceClient(grpcConn)
		clusterClient = clusterpb.NewClusterServiceClient(grpcConn)

		// Wait for leader election
		Eventually(func(g Gomega) bool {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:admin"))
			g.Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())
	})

	Context("with Ed25519 auth enabled", func() {
		It("should allow unauthenticated endpoints without token", func() {
			resp, err := client.Discovery(ctx, &servicepb.DiscoveryRequest{})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject authenticated endpoints without token", func() {
			_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "test"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept requests with valid EdDSA token and correct scope", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:write"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("ed25519-auth-test-ledger", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject requests with wrong scope", func() {
			// Token has only read scope, but Apply requires write
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:read"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("ed25519-auth-test-ledger-2", nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should allow read operations with read scope", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:read"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "ed25519-auth-test-ledger"})
			Expect(err).To(Succeed())
		})

		It("should allow write operations to create transactions", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:write"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("ed25519-auth-test-ledger", []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should allow admin operations with admin scope", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:admin"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.Leader).NotTo(BeZero())
		})

		It("should reject cluster operations without admin scope", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:read"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject tokens signed by unknown keys", func() {
			// Generate a different keypair not configured on the server
			_, unknownPriv, err := ed25519.GenerateKey(rand.Reader)
			Expect(err).To(Succeed())

			token, err := signEdDSAJWT(unknownPriv, "unknown-key", makeEdDSAClaims("ledger:read"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "test"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should reject expired EdDSA tokens", func() {
			claims := makeEdDSAClaims("ledger:read")
			pastTime := time.Now().Add(-1 * time.Hour)
			claims.Expiration = oidc.FromTime(oidc.Time(pastTime.Unix()).AsTime())

			token, err := signEdDSAJWT(edPrivKey, keyID, claims)
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "test"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})
	})
})

// Dedicated port range for Ed25519 scope restriction tests.
const (
	ed25519ScopeTestHTTPPort = 15820
	ed25519ScopeTestGRPCPort = 15920
	ed25519ScopeTestRaftPort = 14820
)

var _ = Describe("Ed25519 Auth Scope Restrictions", Ordered, func() {
	var (
		ctx           context.Context
		grpcConn      *grpc.ClientConn
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		edPrivKey     ed25519.PrivateKey
		keyID         string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		keyID = "read-only-key"
		keysDir := GinkgoT().TempDir()

		// Configure key with read-only scopes (no write or admin)
		var configPath string
		var err error
		edPrivKey, configPath, err = writeEd25519KeysConfig(
			keysDir, keyID, []string{"ledger:read"},
		)
		Expect(err).To(Succeed())

		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			Expect(os.RemoveAll(walTmpDir)).To(Succeed())
			Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
		})

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			HTTPPort:  ed25519ScopeTestHTTPPort,
			RaftPort:  ed25519ScopeTestRaftPort,
			GRPCPort:  ed25519ScopeTestGRPCPort,
			WalDir:    walTmpDir,
			DataDir:   dataTmpDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithAuthEnabled(),
			testserver.WithAuthEd25519Keys(configPath),
			testserver.WithAuthService("ledger"),
		)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		grpcConn, err = grpc.NewClient(
			fmt.Sprintf("localhost:%d", ed25519ScopeTestGRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		client = servicepb.NewBucketServiceClient(grpcConn)
		clusterClient = clusterpb.NewClusterServiceClient(grpcConn)

		// Wait for leader election using a read that goes through Raft.
		// GetLedger calls ReadIndexAndWait, which returns ErrNoLeader (Unavailable)
		// immediately when no leader is elected yet. We expect NotFound once a
		// leader is available (the ledger doesn't exist, but the read succeeds).
		Eventually(func(g Gomega) {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:read"))
			g.Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "wait-for-leader"})
			if err != nil {
				st, ok := status.FromError(err)
				g.Expect(ok).To(BeTrue())
				g.Expect(st.Code()).To(Equal(codes.NotFound))
			}
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	Context("with key restricted to read-only scopes", func() {
		It("should reject token claiming write scope when key only allows read", func() {
			// Token claims write scope, but key only allows read -> scope enforcement rejects
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:write"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("scope-test-ledger", nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			// The token is rejected at validation time because the claimed scope exceeds key allowlist
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should reject token claiming admin scope when key only allows read", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:admin"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should allow read operations with read scope", func() {
			token, err := signEdDSAJWT(edPrivKey, keyID, makeEdDSAClaims("ledger:read"))
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			// GetLedger is a read operation — ledger may not exist but shouldn't be auth error
			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "nonexistent"})
			// Either succeeds or fails with NOT_FOUND, but not auth error
			if err != nil {
				st, ok := status.FromError(err)
				Expect(ok).To(BeTrue())
				Expect(st.Code()).NotTo(Equal(codes.Unauthenticated))
				Expect(st.Code()).NotTo(Equal(codes.PermissionDenied))
			}
		})
	})
})

// Dedicated port range for Ed25519 god-mode tests.
const (
	ed25519GodTestHTTPPort = 15830
	ed25519GodTestGRPCPort = 15930
	ed25519GodTestRaftPort = 14830
)

// writeEd25519GodKeysConfig generates two Ed25519 keypairs — one god-mode key and one regular key —
// and writes the auth-keys.json config file. Returns both private keys and the config file path.
func writeEd25519GodKeysConfig(dir string) (godPriv, regularPriv ed25519.PrivateKey, configPath string, err error) {
	// God key
	_, godPriv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, "", err
	}
	godPub := godPriv.Public().(ed25519.PublicKey)
	godPubFile := filepath.Join(dir, "god-pubkey.hex")
	if err := os.WriteFile(godPubFile, []byte(fmt.Sprintf("%x\n", godPub)), 0644); err != nil {
		return nil, nil, "", err
	}

	// Regular key
	_, regularPriv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, "", err
	}
	regularPub := regularPriv.Public().(ed25519.PublicKey)
	regularPubFile := filepath.Join(dir, "regular-pubkey.hex")
	if err := os.WriteFile(regularPubFile, []byte(fmt.Sprintf("%x\n", regularPub)), 0644); err != nil {
		return nil, nil, "", err
	}

	cfg := internalauth.Ed25519KeysConfig{
		Keys: []internalauth.Ed25519KeyEntry{
			{
				KeyID:         "god-key",
				PublicKeyFile: godPubFile,
				Scopes:        []string{},
				God:           true,
			},
			{
				KeyID:         "regular-key",
				PublicKeyFile: regularPubFile,
				Scopes:        []string{"ledger:read"},
			},
		},
	}
	configData, err := json.Marshal(cfg)
	if err != nil {
		return nil, nil, "", err
	}

	configPath = filepath.Join(dir, "auth-keys.json")
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return nil, nil, "", err
	}

	return godPriv, regularPriv, configPath, nil
}

var _ = Describe("Ed25519 Auth God Mode", Ordered, func() {
	var (
		ctx            context.Context
		grpcConn       *grpc.ClientConn
		client         servicepb.BucketServiceClient
		clusterClient  clusterpb.ClusterServiceClient
		godPrivKey     ed25519.PrivateKey
		regularPrivKey ed25519.PrivateKey
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		keysDir := GinkgoT().TempDir()

		var configPath string
		var err error
		godPrivKey, regularPrivKey, configPath, err = writeEd25519GodKeysConfig(keysDir)
		Expect(err).To(Succeed())

		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			Expect(os.RemoveAll(walTmpDir)).To(Succeed())
			Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
		})

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			HTTPPort:  ed25519GodTestHTTPPort,
			RaftPort:  ed25519GodTestRaftPort,
			GRPCPort:  ed25519GodTestGRPCPort,
			WalDir:    walTmpDir,
			DataDir:   dataTmpDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithAuthEnabled(),
			testserver.WithAuthEd25519Keys(configPath),
			testserver.WithAuthService("ledger"),
		)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		grpcConn, err = grpc.NewClient(
			fmt.Sprintf("localhost:%d", ed25519GodTestGRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		client = servicepb.NewBucketServiceClient(grpcConn)
		clusterClient = clusterpb.NewClusterServiceClient(grpcConn)

		// Wait for leader election
		Eventually(func(g Gomega) bool {
			godClaims := makeEdDSAClaims()
			godClaims.Claims = map[string]any{"god": true}
			token, err := signEdDSAJWT(godPrivKey, "god-key", godClaims)
			g.Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())
	})

	Context("with god-mode key", func() {
		It("should allow admin operations without any scopes", func() {
			claims := makeEdDSAClaims() // no scopes at all
			claims.Claims = map[string]any{"god": true}
			token, err := signEdDSAJWT(godPrivKey, "god-key", claims)
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.Leader).NotTo(BeZero())
		})

		It("should allow write operations without any scopes", func() {
			claims := makeEdDSAClaims()
			claims.Claims = map[string]any{"god": true}
			token, err := signEdDSAJWT(godPrivKey, "god-key", claims)
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("god-mode-test-ledger", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should allow read operations without any scopes", func() {
			claims := makeEdDSAClaims()
			claims.Claims = map[string]any{"god": true}
			token, err := signEdDSAJWT(godPrivKey, "god-key", claims)
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "god-mode-test-ledger"})
			Expect(err).To(Succeed())
		})
	})

	Context("with regular key claiming god mode", func() {
		It("should reject the token", func() {
			claims := makeEdDSAClaims("ledger:read")
			claims.Claims = map[string]any{"god": true}
			token, err := signEdDSAJWT(regularPrivKey, "regular-key", claims)
			Expect(err).To(Succeed())
			authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "test"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})
	})
})
