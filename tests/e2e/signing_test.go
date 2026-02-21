//go:build e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// listAllSigningKeys collects all signing keys from the ListSigningKeys stream into a slice.
func listAllSigningKeys(ctx context.Context, client servicepb.BucketServiceClient) []*commonpb.SigningKey {
	stream, err := client.ListSigningKeys(ctx, &servicepb.ListSigningKeysRequest{})
	Expect(err).To(Succeed())

	var keys []*commonpb.SigningKey
	for {
		key, err := stream.Recv()
		if err == io.EOF {
			break
		}
		Expect(err).To(Succeed())
		keys = append(keys, key)
	}
	return keys
}

// findSigningKey finds a key by ID in a slice of signing keys. Returns nil if not found.
func findSigningKey(keys []*commonpb.SigningKey, keyID string) *commonpb.SigningKey {
	for _, k := range keys {
		if k.KeyId == keyID {
			return k
		}
	}
	return nil
}

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
func revokeSigningKeyAction(keyID string, cascade bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RevokeSigningKey{
			RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{
				KeyId:   keyID,
				Cascade: cascade,
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
					revokeSigningKeyAction("second-key", false),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed RevokeSigningKey", func() {
			req := revokeSigningKeyAction("second-key", false)
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

	Context("ListSigningKeys non-cascade revoke", Ordered, func() {
		var (
			ctx      context.Context
			client   servicepb.BucketServiceClient
			privKeyA ed25519.PrivateKey
		)

		const (
			httpPort = 9204
			grpcPort = 8204
			keyIDA   = "nc-key-A"
			keyIDB   = "nc-key-B"
			keyIDC   = "nc-key-C"
		)

		BeforeAll(func() {
			var pubKeyA, pubKeyB, pubKeyC ed25519.PublicKey
			pubKeyA, privKeyA = generateTestKeypair()
			pubKeyB, _ = generateTestKeypair()
			pubKeyC, _ = generateTestKeypair()

			ctx, client, _ = setupSingleNode(httpPort, grpcPort)

			// Register root key A (bootstrap, unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction(keyIDA, pubKeyA),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register child key B (signed by A)
			reqB := registerSigningKeyAction(keyIDB, pubKeyB)
			signRequest(reqB, keyIDA, privKeyA)
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqB},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register grandchild key C (signed by A, parent is A — not B)
			// This makes C a child of A, so revoking B (non-cascade) leaves C
			reqC := registerSigningKeyAction(keyIDC, pubKeyC)
			signRequest(reqC, keyIDA, privKeyA)
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqC},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should list A, B, C before revoke", func() {
			keys := listAllSigningKeys(ctx, client)
			Expect(keys).To(HaveLen(3))
			Expect(findSigningKey(keys, keyIDA)).NotTo(BeNil())
			Expect(findSigningKey(keys, keyIDB)).NotTo(BeNil())
			Expect(findSigningKey(keys, keyIDC)).NotTo(BeNil())
		})

		It("should list A and C after non-cascade revoke of B", func() {
			req := revokeSigningKeyAction(keyIDB, false)
			signRequest(req, keyIDA, privKeyA)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			keys := listAllSigningKeys(ctx, client)
			Expect(keys).To(HaveLen(2))
			Expect(findSigningKey(keys, keyIDA)).NotTo(BeNil())
			Expect(findSigningKey(keys, keyIDB)).To(BeNil(), "revoked key B should not be listed")
			Expect(findSigningKey(keys, keyIDC)).NotTo(BeNil(), "non-cascaded key C should still be listed")
		})
	})

	Context("Hierarchical key management", Ordered, func() {
		var (
			ctx      context.Context
			client   servicepb.BucketServiceClient
			privKeyA ed25519.PrivateKey
			privKeyB ed25519.PrivateKey
			privKeyC ed25519.PrivateKey
		)

		const (
			httpPort = 9203
			grpcPort = 8203
			keyIDA   = "key-A"
			keyIDB   = "key-B"
			keyIDC   = "key-C"
		)

		BeforeAll(func() {
			var pubKeyA, pubKeyB, pubKeyC ed25519.PublicKey
			pubKeyA, privKeyA = generateTestKeypair()
			pubKeyB, privKeyB = generateTestKeypair()
			pubKeyC, privKeyC = generateTestKeypair()

			ctx, client, _ = setupSingleNode(httpPort, grpcPort)

			// Register root key A (bootstrap, unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					registerSigningKeyAction(keyIDA, pubKeyA),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register child key B (signed by A -> B is child of A)
			reqB := registerSigningKeyAction(keyIDB, pubKeyB)
			signRequest(reqB, keyIDA, privKeyA)

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqB},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			// Verify ParentKeyId in the log
			regLog := resp.Logs[0].Payload.GetRegisterSigningKey()
			Expect(regLog).NotTo(BeNil())
			Expect(regLog.ParentKeyId).To(Equal(keyIDA))

			// Register grandchild key C (signed by B -> C is child of B)
			reqC := registerSigningKeyAction(keyIDC, pubKeyC)
			signRequest(reqC, keyIDB, privKeyB)

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqC},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			// Verify ParentKeyId in the log
			regLog = resp.Logs[0].Payload.GetRegisterSigningKey()
			Expect(regLog).NotTo(BeNil())
			Expect(regLog.ParentKeyId).To(Equal(keyIDB))

			// Create a ledger using key A for later tests
			ledgerReq := createLedgerAction("hierarchy-test", nil)
			signRequest(ledgerReq, keyIDA, privKeyA)
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{ledgerReq},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should list all three keys (A, B, C) with correct parent relationships", func() {
			keys := listAllSigningKeys(ctx, client)
			Expect(keys).To(HaveLen(3))

			keyA := findSigningKey(keys, keyIDA)
			Expect(keyA).NotTo(BeNil())
			Expect(keyA.ParentKeyId).To(BeEmpty(), "root key A should have no parent")

			keyB := findSigningKey(keys, keyIDB)
			Expect(keyB).NotTo(BeNil())
			Expect(keyB.ParentKeyId).To(Equal(keyIDA), "key B should have A as parent")

			keyC := findSigningKey(keys, keyIDC)
			Expect(keyC).NotTo(BeNil())
			Expect(keyC.ParentKeyId).To(Equal(keyIDB), "key C should have B as parent")
		})

		It("should accept requests signed by child key B", func() {
			req := createTransactionAction("hierarchy-test", []*commonpb.Posting{
				newPosting("world", "h-bob", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req, keyIDB, privKeyB)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept requests signed by grandchild key C", func() {
			req := createTransactionAction("hierarchy-test", []*commonpb.Posting{
				newPosting("world", "h-charlie", big.NewInt(100), "USD"),
			}, nil, nil)
			signRequest(req, keyIDC, privKeyC)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should cascade revoke B and C when B is revoked with cascade (signed by A)", func() {
			req := revokeSigningKeyAction(keyIDB, true)
			signRequest(req, keyIDA, privKeyA)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify cascade in the log
			revokeLog := resp.Logs[0].Payload.GetRevokeSigningKey()
			Expect(revokeLog).NotTo(BeNil())
			Expect(revokeLog.KeyId).To(Equal(keyIDB))
			Expect(revokeLog.CascadedKeyIds).To(ContainElement(keyIDC))
		})

		It("should list only key A after cascade revoke of B", func() {
			keys := listAllSigningKeys(ctx, client)
			Expect(keys).To(HaveLen(1))

			keyA := findSigningKey(keys, keyIDA)
			Expect(keyA).NotTo(BeNil())
			Expect(keyA.ParentKeyId).To(BeEmpty())

			Expect(findSigningKey(keys, keyIDB)).To(BeNil(), "revoked key B should not be listed")
			Expect(findSigningKey(keys, keyIDC)).To(BeNil(), "cascade-revoked key C should not be listed")
		})

		It("should still accept requests signed by root key A", func() {
			req := createTransactionAction("hierarchy-test", []*commonpb.Posting{
				newPosting("world", "h-post-revoke", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, keyIDA, privKeyA)

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject requests signed by revoked key B", func() {
			req := createTransactionAction("hierarchy-test", []*commonpb.Posting{
				newPosting("world", "h-revoked-b", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, keyIDB, privKeyB)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests signed by cascade-revoked key C", func() {
			req := createTransactionAction("hierarchy-test", []*commonpb.Posting{
				newPosting("world", "h-revoked-c", big.NewInt(50), "USD"),
			}, nil, nil)
			signRequest(req, keyIDC, privKeyC)

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})
	})
})
