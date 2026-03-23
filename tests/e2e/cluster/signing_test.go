//go:build e2e

package cluster

import (
	"context"
	"crypto/ed25519"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
			var err error
			pubKey, privKey, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)
		})

		It("should accept unsigned requests when no keys exist", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("signing-bootstrap", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should allow unsigned RegisterSigningKey as bootstrap (first key)", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction(keyID, pubKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned RegisterSigningKey once keys exist", func() {
			newPubKey, _, err := actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction("another-key", newPubKey),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed RegisterSigningKey to add a second key", func() {
			newPubKey, _, err := actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			req := actions.RegisterSigningKeyAction("second-key", newPubKey)
			_, err = actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned RevokeSigningKey", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevokeSigningKeyAction("second-key", false),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed RevokeSigningKey", func() {
			req := actions.RevokeSigningKeyAction("second-key", false)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned SetSigningConfig", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SetSigningConfigAction(true),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed SetSigningConfig to enable require-signatures", func() {
			req := actions.SetSigningConfigAction(true)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject unsigned regular requests after require-signatures is enabled", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("signing-should-fail", nil),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept signed regular requests after require-signatures is enabled", func() {
			req := actions.CreateLedgerAction("signing-required-ok", nil)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature).NotTo(BeNil())
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID))
		})

		It("should disable require-signatures via signed config change", func() {
			req := actions.SetSigningConfigAction(false)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Now unsigned requests should work again
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("signing-disabled-again", nil),
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
			var err error
			pubKey, privKey, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Bootstrap: register the first key (unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction(keyID, pubKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create a ledger for transaction tests
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept signed requests and persist signature in log", func() {
			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

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
			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bob", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, "unknown-key-id", privKey)
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests with invalid signature", func() {
			_, wrongPrivKey, err := actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "charlie", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err = actions.SignRequest(req, keyID, wrongPrivKey)
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests with tampered signed_payload", func() {
			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "dave", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyID, privKey)
			Expect(err).To(Succeed())

			// Tamper with the signed_payload after signing
			req.Signature.SignedPayload = append(req.Signature.SignedPayload, 0xFF)

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should handle signed bulk operations", func() {
			req1 := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bulk-alice", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req1, keyID, privKey)
			Expect(err).To(Succeed())

			req2 := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bulk-bob", big.NewInt(200), "USD"),
			}, nil, nil)
			_, err = actions.SignRequest(req2, keyID, privKey)
			Expect(err).To(Succeed())

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
			signedReq := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "mixed-signed", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(signedReq, keyID, privKey)
			Expect(err).To(Succeed())

			unsignedReq := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "mixed-unsigned", big.NewInt(100), "USD"),
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
			var err error
			pubKey1, privKey1, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			pubKey2, privKey2, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Bootstrap: register the first key (unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction(keyID1, pubKey1),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register the second key (signed by first key)
			req := actions.RegisterSigningKeyAction(keyID2, pubKey2)
			_, err = actions.SignRequest(req, keyID1, privKey1)
			Expect(err).To(Succeed())

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Enable require-signatures (signed by first key)
			configReq := actions.SetSigningConfigAction(true)
			_, err = actions.SignRequest(configReq, keyID1, privKey1)
			Expect(err).To(Succeed())

			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{configReq},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept requests signed with the first key", func() {
			req := actions.CreateLedgerAction(ledgerName, nil)
			_, err := actions.SignRequest(req, keyID1, privKey1)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID1))
		})

		It("should accept requests signed with the second key", func() {
			req := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "multi-key-test", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyID2, privKey2)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
			Expect(resp.Logs[0].Signature.KeyId).To(Equal(keyID2))
		})

		It("should accept bulk with different signing keys", func() {
			req1 := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "multi-key-1", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req1, keyID1, privKey1)
			Expect(err).To(Succeed())

			req2 := actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "multi-key-2", big.NewInt(200), "USD"),
			}, nil, nil)
			_, err = actions.SignRequest(req2, keyID2, privKey2)
			Expect(err).To(Succeed())

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
			var err error
			pubKeyA, privKeyA, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			pubKeyB, _, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			pubKeyC, _, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Register root key A (bootstrap, unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction(keyIDA, pubKeyA),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register child key B (signed by A)
			reqB := actions.RegisterSigningKeyAction(keyIDB, pubKeyB)
			_, err = actions.SignRequest(reqB, keyIDA, privKeyA)
			Expect(err).To(Succeed())
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqB},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register grandchild key C (signed by A, parent is A -- not B)
			// This makes C a child of A, so revoking B (non-cascade) leaves C
			reqC := actions.RegisterSigningKeyAction(keyIDC, pubKeyC)
			_, err = actions.SignRequest(reqC, keyIDA, privKeyA)
			Expect(err).To(Succeed())
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{reqC},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should list A, B, C before revoke", func() {
			keys, err := actions.ListAllSigningKeys(ctx, client)
			Expect(err).To(Succeed())
			Expect(keys).To(HaveLen(3))
			Expect(actions.FindSigningKey(keys, keyIDA)).NotTo(BeNil())
			Expect(actions.FindSigningKey(keys, keyIDB)).NotTo(BeNil())
			Expect(actions.FindSigningKey(keys, keyIDC)).NotTo(BeNil())
		})

		It("should list A and C after non-cascade revoke of B", func() {
			req := actions.RevokeSigningKeyAction(keyIDB, false)
			_, err := actions.SignRequest(req, keyIDA, privKeyA)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			keys, err := actions.ListAllSigningKeys(ctx, client)
			Expect(err).To(Succeed())
			Expect(keys).To(HaveLen(2))
			Expect(actions.FindSigningKey(keys, keyIDA)).NotTo(BeNil())
			Expect(actions.FindSigningKey(keys, keyIDB)).To(BeNil(), "revoked key B should not be listed")
			Expect(actions.FindSigningKey(keys, keyIDC)).NotTo(BeNil(), "non-cascaded key C should still be listed")
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
			var err error
			pubKeyA, privKeyA, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			pubKeyB, privKeyB, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())
			pubKeyC, privKeyC, err = actions.GenerateTestKeypair()
			Expect(err).To(Succeed())

			ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

			// Register root key A (bootstrap, unsigned)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RegisterSigningKeyAction(keyIDA, pubKeyA),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Register child key B (signed by A -> B is child of A)
			reqB := actions.RegisterSigningKeyAction(keyIDB, pubKeyB)
			_, err = actions.SignRequest(reqB, keyIDA, privKeyA)
			Expect(err).To(Succeed())

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
			reqC := actions.RegisterSigningKeyAction(keyIDC, pubKeyC)
			_, err = actions.SignRequest(reqC, keyIDB, privKeyB)
			Expect(err).To(Succeed())

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
			ledgerReq := actions.CreateLedgerAction("hierarchy-test", nil)
			_, err = actions.SignRequest(ledgerReq, keyIDA, privKeyA)
			Expect(err).To(Succeed())
			resp, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{ledgerReq},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should list all three keys (A, B, C) with correct parent relationships", func() {
			keys, err := actions.ListAllSigningKeys(ctx, client)
			Expect(err).To(Succeed())
			Expect(keys).To(HaveLen(3))

			keyA := actions.FindSigningKey(keys, keyIDA)
			Expect(keyA).NotTo(BeNil())
			Expect(keyA.ParentKeyId).To(BeEmpty(), "root key A should have no parent")

			keyB := actions.FindSigningKey(keys, keyIDB)
			Expect(keyB).NotTo(BeNil())
			Expect(keyB.ParentKeyId).To(Equal(keyIDA), "key B should have A as parent")

			keyC := actions.FindSigningKey(keys, keyIDC)
			Expect(keyC).NotTo(BeNil())
			Expect(keyC.ParentKeyId).To(Equal(keyIDB), "key C should have B as parent")
		})

		It("should accept requests signed by child key B", func() {
			req := actions.CreateTransactionAction("hierarchy-test", []*commonpb.Posting{
				actions.NewPosting("world", "h-bob", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyIDB, privKeyB)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should accept requests signed by grandchild key C", func() {
			req := actions.CreateTransactionAction("hierarchy-test", []*commonpb.Posting{
				actions.NewPosting("world", "h-charlie", big.NewInt(100), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyIDC, privKeyC)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should cascade revoke B and C when B is revoked with cascade (signed by A)", func() {
			req := actions.RevokeSigningKeyAction(keyIDB, true)
			_, err := actions.SignRequest(req, keyIDA, privKeyA)
			Expect(err).To(Succeed())

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
			keys, err := actions.ListAllSigningKeys(ctx, client)
			Expect(err).To(Succeed())
			Expect(keys).To(HaveLen(1))

			keyA := actions.FindSigningKey(keys, keyIDA)
			Expect(keyA).NotTo(BeNil())
			Expect(keyA.ParentKeyId).To(BeEmpty())

			Expect(actions.FindSigningKey(keys, keyIDB)).To(BeNil(), "revoked key B should not be listed")
			Expect(actions.FindSigningKey(keys, keyIDC)).To(BeNil(), "cascade-revoked key C should not be listed")
		})

		It("should still accept requests signed by root key A", func() {
			req := actions.CreateTransactionAction("hierarchy-test", []*commonpb.Posting{
				actions.NewPosting("world", "h-post-revoke", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyIDA, privKeyA)
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("should reject requests signed by revoked key B", func() {
			req := actions.CreateTransactionAction("hierarchy-test", []*commonpb.Posting{
				actions.NewPosting("world", "h-revoked-b", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyIDB, privKeyB)
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should reject requests signed by cascade-revoked key C", func() {
			req := actions.CreateTransactionAction("hierarchy-test", []*commonpb.Posting{
				actions.NewPosting("world", "h-revoked-c", big.NewInt(50), "USD"),
			}, nil, nil)
			_, err := actions.SignRequest(req, keyIDC, privKeyC)
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{req},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})
	})
})
