//go:build e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// writeTestSeedFile writes an Ed25519 seed to a temp file and returns the path.
func writeTestSeedFile(seed []byte) string {
	dir := GinkgoT().TempDir()
	seedPath := filepath.Join(dir, "seed.hex")
	Expect(os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)+"\n"), 0600)).To(Succeed())
	return seedPath
}

var _ = Describe("Response Signing", func() {

	Context("Server with response signing enabled", Ordered, func() {
		var (
			ctx       context.Context
			client    servicepb.BucketServiceClient
			seed      []byte
			publicKey ed25519.PublicKey
		)

		const (
			httpPort   = 9230
			grpcPort   = 8230
			ledgerName = "response-signing-test"
		)

		BeforeAll(func() {
			// Generate an Ed25519 keypair for response signing
			seed = make([]byte, ed25519.SeedSize)
			_, err := rand.Read(seed)
			Expect(err).To(Succeed())

			privKey := ed25519.NewKeyFromSeed(seed)
			publicKey = privKey.Public().(ed25519.PublicKey)

			seedPath := writeTestSeedFile(seed)

			ctx, client, _ = setupSingleNode(httpPort, grpcPort,
				testserver.WithResponseSigningKey(seedPath),
			)

			// Create a test ledger
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should include response signature in Apply response logs", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			Expect(log.ResponseSignature).NotTo(BeNil())
			Expect(log.ResponseSignature.Signature).To(HaveLen(ed25519.SignatureSize))
			Expect(log.ResponseSignature.SignedPayload).NotTo(BeEmpty())
			Expect(log.ResponseSignature.KeyId).NotTo(BeEmpty())
		})

		It("should produce verifiable response signatures", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the response signature using the known public key
			sig := resp.Logs[0].ResponseSignature
			Expect(sig).NotTo(BeNil())
			Expect(signing.VerifyResponseSignature(sig, publicKey)).To(Succeed())
		})

		It("should fail verification with a wrong public key", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(50), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify with a different key should fail
			_, wrongPrivKey, err := ed25519.GenerateKey(rand.Reader)
			Expect(err).To(Succeed())
			wrongPubKey := wrongPrivKey.Public().(ed25519.PublicKey)

			sig := resp.Logs[0].ResponseSignature
			Expect(sig).NotTo(BeNil())
			Expect(signing.VerifyResponseSignature(sig, wrongPubKey)).To(HaveOccurred())
		})

		It("should sign all logs in bulk Apply response", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-1", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-2", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			for _, log := range resp.Logs {
				Expect(log.ResponseSignature).NotTo(BeNil())
				Expect(signing.VerifyResponseSignature(log.ResponseSignature, publicKey)).To(Succeed())
			}
		})

		It("should expose public key via Discovery RPC", func() {
			discoveryResp, err := client.Discovery(ctx, &servicepb.DiscoveryRequest{})
			Expect(err).To(Succeed())
			Expect(discoveryResp.ResponseSigning).NotTo(BeNil())
			Expect(discoveryResp.ResponseSigning.PublicKey).To(Equal([]byte(publicKey)))
			Expect(discoveryResp.ResponseSigning.KeyId).NotTo(BeEmpty())
		})

		It("should include ledger creation log in response signatures", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("response-signing-create-test", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			sig := resp.Logs[0].ResponseSignature
			Expect(sig).NotTo(BeNil())
			Expect(signing.VerifyResponseSignature(sig, publicKey)).To(Succeed())
		})
	})

	Context("Server without response signing", Ordered, func() {
		var (
			ctx    context.Context
			client servicepb.BucketServiceClient
		)

		const (
			httpPort   = 9231
			grpcPort   = 8231
			ledgerName = "no-response-signing"
		)

		BeforeAll(func() {
			ctx, client, _ = setupSingleNode(httpPort, grpcPort)

			// Create a test ledger
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should not include response signature in Apply response logs", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].ResponseSignature).To(BeNil())
		})

		It("should return nil response_signing in Discovery RPC", func() {
			discoveryResp, err := client.Discovery(ctx, &servicepb.DiscoveryRequest{})
			Expect(err).To(Succeed())
			Expect(discoveryResp.ResponseSigning).To(BeNil())
		})
	})
})
