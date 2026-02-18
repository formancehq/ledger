//go:build e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// generateTestKeypair generates an Ed25519 keypair and returns (publicKey, privateKey).
func generateTestKeypair() (ed25519.PublicKey, ed25519.PrivateKey) {
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	Expect(err).To(Succeed())
	return pubKey, privKey
}

// signRequest signs a request with the given key and returns the same request (modified in place).
func signRequest(req *servicepb.Request, keyID string, privKey ed25519.PrivateKey) *servicepb.Request {
	Expect(signing.Sign(req, keyID, privKey)).To(Succeed())
	return req
}

// registerSigningKeyAction creates a RegisterSigningKey request.
func registerSigningKeyAction(keyID string, pubKey ed25519.PublicKey) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RegisterSigningKey{
			RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
				KeyId:     keyID,
				PublicKey: []byte(pubKey),
			},
		},
	}
}

// revokeSigningKeyAction creates a RevokeSigningKey request.
func revokeSigningKeyAction(keyID string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RevokeSigningKey{
			RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
				KeyId: keyID,
			},
		},
	}
}

// setSigningConfigAction creates a SetSigningConfig request.
func setSigningConfigAction(requireSignatures bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetSigningConfig{
			SetSigningConfig: &servicepb.SetSigningConfigRequest{
				RequireSignatures: requireSignatures,
			},
		},
	}
}

var _ = Describe("Request Signing", func() {

	Context("Bootstrap and key management via API", Ordered, func() {
		var (
			ctx     context.Context
			client  servicepb.BucketServiceClient
			pubKey  ed25519.PublicKey
			privKey ed25519.PrivateKey
		)

		const (
			httpPort = 9200
			grpcPort = 8200
			keyID    = "admin-key"
		)

		BeforeAll(func() {
			pubKey, privKey = generateTestKeypair()
			ctx, client, _ = setupSingleNode(httpPort, grpcPort)
		})

		It("should accept unsigned requests when no keys exist", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("signing-bootstrap", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should allow unsigned RegisterSigningKey as bootstrap (first key)", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction(keyID, pubKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned RegisterSigningKey once keys exist", func() {
			newPubKey, _ := generateTestKeypair()
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction("another-key", newPubKey),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed RegisterSigningKey to add a second key", func() {
			newPubKey, _ := generateTestKeypair()
			req := registerSigningKeyAction("second-key", newPubKey)
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned RevokeSigningKey", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					revokeSigningKeyAction("second-key"),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed RevokeSigningKey", func() {
			req := revokeSigningKeyAction("second-key")
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned SetSigningConfig", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					setSigningConfigAction(true),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed SetSigningConfig to enable require-signatures", func() {
			req := setSigningConfigAction(true)
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned regular requests after require-signatures is enabled", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("signing-should-fail", nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed regular requests after require-signatures is enabled", func() {
			req := createLedgerAction("signing-required-ok", nil)
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature).NotTo(BeNil())
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID))
		})

		It("should disable require-signatures via signed config change", func() {
			req := setSigningConfigAction(false)
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Now unsigned requests should work again
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction("signing-disabled-again", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})

	Context("Signature verification", Ordered, func() {
		var (
			ctx     context.Context
			client  servicepb.BucketServiceClient
			privKey ed25519.PrivateKey
		)

		const (
			httpPort   = 9201
			grpcPort   = 8201
			ledgerName = "signing-verification"
			keyID      = "verify-key"
		)

		BeforeAll(func() {
			var pubKey ed25519.PublicKey
			pubKey, privKey = generateTestKeypair()

			ctx, client, _ = setupSingleNode(httpPort, grpcPort)

			// Bootstrap: register the first key (unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction(keyID, pubKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create a ledger for transaction tests
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept signed requests and persist signature in log", func() {
			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			Expect(log.Signature).NotTo(BeNil())
			Expect(log.Signature.KeyId).To(Equal(keyID))
			Expect(log.Signature.Signature).To(HaveLen(ed25519.SignatureSize))
			Expect(log.Signature.SignedPayload).NotTo(BeEmpty())
		})

		It("should reject requests signed with an unknown key ID", func() {
			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "bob", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, "unknown-key-id", privKey)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests with invalid signature", func() {
			_, wrongPrivKey := generateTestKeypair()

			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "charlie", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, keyID, wrongPrivKey)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests with tampered signed_payload", func() {
			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "dave", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, keyID, privKey)

			// Tamper with the signed_payload after signing
			req.Signature.SignedPayload = append(req.Signature.SignedPayload, 0xFF)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should handle signed bulk operations", func() {
			req1 := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "bulk-alice", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req1, keyID, privKey)

			req2 := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "bulk-bob", big.NewInt(200), "USD"),
			}, nil, nil)
			signRequest(req2, keyID, privKey)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req1, req2},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			for _, log := range resp.Logs {
				Expect(log.Signature).NotTo(BeNil())
				Expect(log.Signature.KeyId).To(Equal(keyID))
				Expect(log.Signature.Signature).To(HaveLen(ed25519.SignatureSize))
			}
		})

		It("should accept mixed signed and unsigned requests when signatures are not required", func() {
			signedReq := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "mixed-signed", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(signedReq, keyID, privKey)

			unsignedReq := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "mixed-unsigned", big.NewInt(100), "USD"),
			}, nil, nil)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{signedReq, unsignedReq},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			Expect(resp.Logs[0].Signature).NotTo(BeNil())
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID))
			Expect(resp.Logs[1].Signature).To(BeNil())
		})
	})

	Context("Multiple signing keys via API", Ordered, func() {
		var (
			ctx      context.Context
			client   servicepb.BucketServiceClient
			privKey1 ed25519.PrivateKey
			privKey2 ed25519.PrivateKey
		)

		const (
			httpPort   = 9202
			grpcPort   = 8202
			ledgerName = "signing-multi-keys"
			keyID1     = "key-1"
			keyID2     = "key-2"
		)

		BeforeAll(func() {
			var pubKey1, pubKey2 ed25519.PublicKey
			pubKey1, privKey1 = generateTestKeypair()
			pubKey2, privKey2 = generateTestKeypair()

			ctx, client, _ = setupSingleNode(httpPort, grpcPort)

			// Bootstrap: register the first key (unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction(keyID1, pubKey1),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register the second key (signed by first key)
			req := registerSigningKeyAction(keyID2, pubKey2)
			signRequest(req, keyID1, privKey1)

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Enable require-signatures (signed by first key)
			configReq := setSigningConfigAction(true)
			signRequest(configReq, keyID1, privKey1)

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{configReq},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept requests signed with the first key", func() {
			req := createLedgerAction(ledgerName, nil)
			signRequest(req, keyID1, privKey1)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID1))
		})

		It("should accept requests signed with the second key", func() {
			req := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "multi-key-test", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req, keyID2, privKey2)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID2))
		})

		It("should accept bulk with different signing keys", func() {
			req1 := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "multi-key-1", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req1, keyID1, privKey1)

			req2 := createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "multi-key-2", big.NewInt(200), "USD"),
			}, nil, nil)
			signRequest(req2, keyID2, privKey2)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req1, req2},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID1))
			Expect(resp.Logs[1].Signature.KeyId).To(Equal(keyID2))
		})
	})
})
