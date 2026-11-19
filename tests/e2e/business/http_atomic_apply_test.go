//go:build e2e

package business

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("HTTP atomic apply", func() {
	var node *testutil.ServiceWithClient
	var private ed25519.PrivateKey
	var public ed25519.PublicKey
	var envelope *signaturepb.SignedApplyBatch
	var bigintStrings bool
	client := &http.Client{Timeout: 10 * time.Second}
	transaction := func(ledger, source, destination string) any {
		return map[string]any{"apply": map[string]any{"ledger": ledger, "action": map[string]any{"createTransaction": map[string]any{"postings": []any{map[string]any{"source": source, "destination": destination, "amount": 100, "asset": "USD"}}}}}}
	}
	send := func(key string, operations ...any) (int, []byte) {
		GinkgoHelper()
		var body any = map[string]any{"requests": operations}
		if envelope != nil {
			body = map[string]any{"request": body, "payload": envelope.GetPayload()}
		}
		raw, err := json.Marshal(body)
		Expect(err).To(Succeed())
		request, err := http.NewRequestWithContext(GinkgoT().Context(), http.MethodPost, fmt.Sprintf("http://localhost:%d/v3/_/apply", node.HTTPPort), bytes.NewReader(raw))
		Expect(err).To(Succeed())
		request.Header.Set("Content-Type", "application/json")
		if bigintStrings {
			request.Header.Set("Formance-Bigint-As-String", "true")
		}
		request.Header.Set("Idempotency-Key", key)
		if envelope != nil {
			request.Header.Set("Formance-Signing-Key-Id", envelope.GetKeyId())
			request.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(envelope.GetSignature()))
		}
		response, err := client.Do(request)
		Expect(err).To(Succeed())
		defer func() { Expect(response.Body.Close()).To(Succeed()) }()
		raw, err = io.ReadAll(response.Body)
		Expect(err).To(Succeed())

		return response.StatusCode, raw
	}
	BeforeEach(func() {
		var err error
		public, private, err = actions.GenerateTestKeypair()
		Expect(err).To(Succeed())
		seedPath := filepath.Join(GinkgoT().TempDir(), "seed.hex")
		Expect(os.WriteFile(seedPath, fmt.Appendf(nil, "%x\n", private.Seed()), 0600)).To(Succeed())
		_, node = testutil.SetupSingleNode(testserver.WithResponseSigningKey(seedPath))
		envelope = nil
		bigintStrings = false
		_, err = node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("first", nil), actions.CreateLedgerAction("second", nil), actions.RegisterSigningKeyAction("atomic-key", public)))
		Expect(err).To(Succeed())
	})
	It("commits cross-ledger operations in one ordered audit entry", func() {
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		code, raw := send("", transaction("first", "world", "alice"), transaction("second", "world", "bob"))
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var response struct {
			Data struct {
				Logs []struct {
					Payload struct{ Apply struct{ LedgerName string } }
				}
			}
		}
		Expect(json.Unmarshal(raw, &response)).To(Succeed())
		Expect(response.Data.Logs).To(HaveLen(2))
		Expect(response.Data.Logs[0].Payload.Apply.LedgerName).To(Equal("first"))
		Expect(response.Data.Logs[1].Payload.Apply.LedgerName).To(Equal("second"))
		for _, ledger := range []string{"first", "second"} {
			transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(1))
		}
		after, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		Expect(after).To(HaveLen(len(before) + 1))
		entry, err := node.Client.GetAuditEntry(GinkgoT().Context(), &servicepb.GetAuditEntryRequest{Sequence: after[len(after)-1].GetSequence()})
		Expect(err).To(Succeed())
		Expect(entry.GetItems()).To(HaveLen(2))
		Expect(entry.GetItems()[0].GetOrderIndex()).To(Equal(uint32(0)))
		Expect(entry.GetItems()[1].GetOrderIndex()).To(Equal(uint32(1)))
		Expect(entry.GetItems()[0].GetLogSequence()).To(BeNumerically("<", entry.GetItems()[1].GetLogSequence()))
	})
	It("preserves exact amounts and timestamps with signed string presentation", func() {
		const amount = "9007199254740993123456789"
		const timestamp = "2025-06-07T08:09:10.123456Z"
		operation := map[string]any{"apply": map[string]any{"ledger": "first", "action": map[string]any{"createTransaction": map[string]any{"postings": []any{map[string]any{"source": "world", "destination": "exact", "amount": json.Number(amount), "asset": "USD"}}, "timestamp": timestamp}}}}
		bigintStrings = true
		code, raw := send("exact-key", operation)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var result struct {
			Data struct {
				Logs []struct {
					Payload struct {
						Apply struct {
							Log struct {
								Data struct {
									Transaction struct {
										Timestamp string
										Postings  []struct{ Amount string }
									}
								}
							}
						}
					}
					ResponseSignature *signaturepb.SignedLog `json:"responseSignature"`
				}
			}
		}
		Expect(json.Unmarshal(raw, &result)).To(Succeed())
		Expect(result.Data.Logs).To(HaveLen(1))
		log := result.Data.Logs[0]
		Expect(log.Payload.Apply.Log.Data.Transaction.Timestamp).To(Equal(timestamp))
		Expect(log.Payload.Apply.Log.Data.Transaction.Postings).To(HaveLen(1))
		Expect(log.Payload.Apply.Log.Data.Transaction.Postings[0].Amount).To(Equal(amount))
		Expect(signing.VerifyResponseSignature(log.ResponseSignature, public)).To(Succeed())
		authenticated := &commonpb.Log{}
		Expect(authenticated.UnmarshalVT(log.ResponseSignature.GetPayload())).To(Succeed())
		created := authenticated.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
		Expect(created.GetPostings()[0].GetAmount().ToBigInt().String()).To(Equal(amount))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, "first")
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
		Expect(transactions[0].GetPostings()[0].GetAmount().ToBigInt().String()).To(Equal(amount))
	})

	It("returns the assigned ledger identity and initial metadata with its proof", func() {
		code, raw := send("", map[string]any{"createLedger": map[string]any{"name": "created-http", "metadata": map[string]any{"owner": map[string]any{"stringValue": "alice"}}}})
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var result struct {
			Data struct {
				Logs []struct {
					Payload struct {
						CreateLedger struct {
							ID       uint32
							Name     string
							Metadata map[string]string
						}
					}
					ResponseSignature *signaturepb.SignedLog `json:"responseSignature"`
				}
			}
		}
		Expect(json.Unmarshal(raw, &result)).To(Succeed())
		Expect(result.Data.Logs).To(HaveLen(1))
		log := result.Data.Logs[0]
		Expect(log.Payload.CreateLedger.ID).NotTo(BeZero())
		Expect(log.Payload.CreateLedger.Name).To(Equal("created-http"))
		Expect(log.Payload.CreateLedger.Metadata).To(Equal(map[string]string{"owner": "alice"}))
		Expect(signing.VerifyResponseSignature(log.ResponseSignature, public)).To(Succeed())
		authenticated := &commonpb.Log{}
		Expect(authenticated.UnmarshalVT(log.ResponseSignature.GetPayload())).To(Succeed())
		Expect(authenticated.GetPayload().GetCreateLedger().GetId()).To(Equal(log.Payload.CreateLedger.ID))
		Expect(authenticated.GetPayload().GetCreateLedger().GetMetadata()["owner"].GetStringValue()).To(Equal("alice"))
	})
	It("waits for a fresh checkpoint and replays its historical result after deletion", func() {
		operations := []any{transaction("first", "world", "checkpoint"), map[string]any{"createQueryCheckpoint": map[string]any{}}}
		code, raw := send("checkpoint-key", operations...)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var result struct {
			Data struct {
				Logs []struct {
					Payload struct{ CreatedQueryCheckpoint struct{ CheckpointID uint64 } }
				}
			}
		}
		Expect(json.Unmarshal(raw, &result)).To(Succeed())
		Expect(result.Data.Logs).To(HaveLen(2))
		checkpointID := result.Data.Logs[1].Payload.CreatedQueryCheckpoint.CheckpointID
		Expect(checkpointID).NotTo(BeZero())
		// Read immediately, without eventual retries: HTTP must wait for local readiness.
		read := &servicepb.GetTransactionRequest{Ledger: "first", TransactionId: 1, CheckpointId: checkpointID}
		atCheckpoint, err := node.Client.GetTransaction(GinkgoT().Context(), read)
		Expect(err).To(Succeed())
		Expect(atCheckpoint.GetTransaction().GetPostings()[0].GetDestination()).To(Equal("checkpoint"))
		code, raw = send("", map[string]any{"deleteQueryCheckpoint": map[string]any{"checkpointId": checkpointID}})
		Expect(code).To(Equal(http.StatusOK), string(raw))
		_, err = node.Client.GetTransaction(GinkgoT().Context(), read)
		Expect(status.Code(err)).To(Equal(codes.NotFound))
		code, raw = send("checkpoint-key", operations...)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		Expect(json.Unmarshal(raw, &result)).To(Succeed())
		Expect(result.Data.Logs[1].Payload.CreatedQueryCheckpoint.CheckpointID).To(Equal(checkpointID))
		_, err = node.Client.GetTransaction(GinkgoT().Context(), read)
		Expect(status.Code(err)).To(Equal(codes.NotFound))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, "first")
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})

	It("rolls back earlier ledger work when a later operation is rejected", func() {
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		code, raw := send("", transaction("first", "world", "alice"), transaction("second", "unfunded", "bob"))
		Expect(code).To(Equal(http.StatusBadRequest), string(raw))
		Expect(string(raw)).To(ContainSubstring("INSUFFICIENT"))
		for _, ledger := range []string{"first", "second"} {
			transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
			Expect(err).To(Succeed())
			Expect(transactions).To(BeEmpty())
		}
		after, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		Expect(after).To(HaveLen(len(before) + 1))
		Expect(after[len(after)-1].GetSuccess()).To(BeNil())
	})
	It("commits configuration and ledger operations together", func() {
		signed, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{actions.SetSigningConfigAction(true), actions.CreateTransactionAction("first", []*commonpb.Posting{actions.NewPosting("world", "configured", big.NewInt(100), "USD")}, nil, nil)}}, "atomic-key", private)
		Expect(err).To(Succeed())
		envelope = signed.GetSigned()
		code, raw := send("", map[string]any{"setSigningConfig": map[string]any{"requireSignatures": true}}, transaction("first", "world", "configured"))
		Expect(code).To(Equal(http.StatusOK), string(raw))
		envelope = nil
		code, raw = send("", transaction("second", "world", "unsigned"))
		Expect(code).To(Equal(http.StatusUnauthorized), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, "first")
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})
	It("replays a batch across HTTP and gRPC and rejects changed intent", func() {
		code, raw := send("batch-key", transaction("first", "world", "alice"), transaction("second", "world", "bob"))
		Expect(code).To(Equal(http.StatusOK), string(raw))
		repeatedCode, repeated := send("batch-key", transaction("first", "world", "alice"), transaction("second", "world", "bob"))
		Expect(repeatedCode).To(Equal(http.StatusOK), string(repeated))
		var original, replayed struct {
			Data struct{ Logs []map[string]any }
		}
		Expect(json.Unmarshal(raw, &original)).To(Succeed())
		Expect(json.Unmarshal(repeated, &replayed)).To(Succeed())
		for _, logs := range [][]map[string]any{original.Data.Logs, replayed.Data.Logs} {
			for _, log := range logs {
				delete(log, "responseSignature")
			}
		}
		Expect(replayed).To(Equal(original))
		requests := []*servicepb.Request{
			actions.CreateTransactionAction("first", []*commonpb.Posting{actions.NewPosting("world", "alice", big.NewInt(100), "USD")}, nil, nil),
			actions.CreateTransactionAction("second", []*commonpb.Posting{actions.NewPosting("world", "bob", big.NewInt(100), "USD")}, nil, nil),
		}
		replay, err := node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("batch-key", requests...))
		Expect(err).To(Succeed())
		Expect(replay.GetLogs()).To(HaveLen(2))
		code, raw = send("batch-key", transaction("first", "world", "changed"), transaction("second", "world", "bob"))
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		for _, ledger := range []string{"first", "second"} {
			transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
			Expect(err).To(Succeed())
			Expect(transactions).To(HaveLen(1))
		}
	})
	It("binds signed batch intent and preserves the original audit proof", func() {
		request := actions.CreateTransactionAction("first", []*commonpb.Posting{actions.NewPosting("world", "signed", big.NewInt(100), "USD")}, nil, nil)
		signed, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{request}, IdempotencyKey: "signed-key"}, "atomic-key", private)
		Expect(err).To(Succeed())
		envelope = signed.GetSigned()
		code, raw := send("signed-key", transaction("first", "world", "signed"))
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var result struct {
			Data struct {
				Logs []struct {
					ResponseSignature *signaturepb.SignedLog `json:"responseSignature"`
				}
			}
		}
		Expect(json.Unmarshal(raw, &result)).To(Succeed())
		Expect(result.Data.Logs).To(HaveLen(1))
		Expect(signing.VerifyResponseSignature(result.Data.Logs[0].ResponseSignature, public)).To(Succeed())
		code, raw = send("signed-key", transaction("second", "world", "signed"))
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		code, raw = send("changed-key", transaction("first", "world", "signed"))
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		entries, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		entry, err := node.Client.GetAuditEntry(GinkgoT().Context(), &servicepb.GetAuditEntryRequest{Sequence: entries[len(entries)-1].GetSequence()})
		Expect(err).To(Succeed())
		Expect(entry.GetSignature().GetPayload()).To(Equal(envelope.GetPayload()))
		Expect(entry.GetSignature().GetSignature()).To(Equal(envelope.GetSignature()))
		Expect(ed25519.Verify(public, entry.GetSignature().GetPayload(), entry.GetSignature().GetSignature())).To(BeTrue())
		config, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: []*servicepb.Request{actions.SetSigningConfigAction(true)}}, "atomic-key", private)
		Expect(err).To(Succeed())
		_, err = node.Client.Apply(GinkgoT().Context(), config)
		Expect(err).To(Succeed())
		code, raw = send("signed-key", transaction("first", "world", "signed"))
		Expect(code).To(Equal(http.StatusOK), string(raw))
		envelope = nil
		envelope = nil
		code, raw = send("", transaction("second", "world", "unsigned"))
		Expect(code).To(Equal(http.StatusUnauthorized), string(raw))
	})

})

var _ = Describe("HTTP atomic apply scopes", func() {
	It("checks every operation before committing any part of the batch", func() {
		ctx := GinkgoT().Context()
		dir := GinkgoT().TempDir()
		public, private, err := ed25519.GenerateKey(rand.Reader)
		Expect(err).To(Succeed())
		publicPath := filepath.Join(dir, "public.hex")
		Expect(os.WriteFile(publicPath, fmt.Appendf(nil, "%x\n", public), 0600)).To(Succeed())
		config, err := json.Marshal(internalauth.Ed25519KeysConfig{Keys: []internalauth.Ed25519KeyEntry{{
			KeyID: "http-atomic-scope-key", PublicKeyFile: publicPath,
			Scopes: []string{"ledger:admin", "ledger:ReadOps", "ledger:WriteOps", "ledger:WriteTransaction", "ledger:WriteLedger", "ledger:ReadAudit"},
		}}})
		Expect(err).To(Succeed())
		configPath := filepath.Join(dir, "auth.json")
		Expect(os.WriteFile(configPath, config, 0600)).To(Succeed())
		token := func(scope string) string {
			GinkgoHelper()
			claims := &oidc.AccessTokenClaims{}
			claims.Subject = "http-atomic-scope-user"
			claims.IssuedAt = oidc.FromTime(oidc.Time(time.Now().Unix()).AsTime())
			claims.Expiration = oidc.FromTime(oidc.Time(time.Now().Add(time.Hour).Unix()).AsTime())
			claims.Scopes = oidc.SpaceDelimitedArray(strings.Fields(scope))
			payload, err := json.Marshal(claims)
			Expect(err).To(Succeed())
			signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA,
				Key: &jose.JSONWebKey{Key: private, KeyID: "http-atomic-scope-key"}}, nil)
			Expect(err).To(Succeed())
			signed, err := signer.Sign(payload)
			Expect(err).To(Succeed())
			raw, err := signed.CompactSerialize()
			Expect(err).To(Succeed())

			return raw
		}
		certs, err := testserver.GenerateTestCerts(dir)
		Expect(err).To(Succeed())
		lease := testserver.AllocateNodeLease()
		ports := lease.Ports()
		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID: 1, ClusterID: "http-atomic-scope-cluster", Ports: ports,
			WalDir: GinkgoT().TempDir(), DataDir: GinkgoT().TempDir(), Output: GinkgoWriter,
		})
		instruments = append(instruments, testserver.WithBootstrap(), testserver.WithAuthEnabled(),
			testserver.WithAuthEd25519Keys(configPath), testserver.WithAuthService("ledger"),
			testserver.WithTLSMode("required"), testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile))
		server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})
		ca, err := os.ReadFile(certs.CACertFile)
		Expect(err).To(Succeed())
		roots := x509.NewCertPool()
		Expect(roots.AppendCertsFromPEM(ca)).To(BeTrue())
		tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", ports.GRPC()),
			grpcprotocol.ClientOption(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })
		cluster := clusterpb.NewClusterServiceClient(conn)
		adminCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token("ledger:admin"))
		Eventually(func(g Gomega) uint64 {
			state, err := cluster.GetClusterState(adminCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())

			return state.GetLeader()
		}).Within(10 * time.Second).ShouldNot(BeZero())
		// Leadership is visible before its first write-gate verdict. Probe with
		// invalid input so readiness cannot create business state.
		bucket := servicepb.NewBucketServiceClient(conn)
		probe := servicepb.UnsignedApplyRequest("write-gate-probe", actions.CreateLedgerAction("", nil))
		probeCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token("ledger:WriteLedger"))
		Eventually(func() codes.Code {
			_, err := bucket.Apply(probeCtx, probe)

			return status.Code(err)
		}).Within(5 * time.Second).ProbeEvery(10 * time.Millisecond).Should(Equal(codes.InvalidArgument))
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		DeferCleanup(transport.CloseIdleConnections)
		client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
		request := func(method, suffix, scope string, body []byte) (int, []byte) {
			GinkgoHelper()
			req, err := http.NewRequestWithContext(ctx, method,
				fmt.Sprintf("http://localhost:%d/v3/_/apply%s", ports.HTTP(), suffix), bytes.NewReader(body))
			Expect(err).To(Succeed())
			req.Header.Set("Content-Type", "application/json")
			if scope != "" {
				req.Header.Set("Authorization", "Bearer "+token(scope))
			}
			resp, err := client.Do(req)
			Expect(err).To(Succeed())
			defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
			raw, err := io.ReadAll(resp.Body)
			Expect(err).To(Succeed())

			return resp.StatusCode, raw
		}
		body, err := json.Marshal(map[string]any{"requests": []any{map[string]any{"createLedger": map[string]any{"name": "scope-ledger"}}, map[string]any{"registerSigningKey": map[string]any{"keyId": "scope-root", "publicKey": public}}}})
		Expect(err).To(Succeed())
		auditCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token("ledger:ReadAudit"))
		before, err := actions.ListAuditEntries(auditCtx, bucket, false)
		Expect(err).To(Succeed())
		for _, scope := range []string{"ledger:WriteLedger", "ledger:WriteOps"} {
			code, raw := request(http.MethodPost, "", scope, body)
			Expect(code).To(Equal(http.StatusForbidden), string(raw))
		}
		after, err := actions.ListAuditEntries(auditCtx, bucket, false)
		Expect(err).To(Succeed())
		Expect(after).To(HaveLen(len(before)))
		code, raw := request(http.MethodPost, "", "ledger:WriteLedger ledger:WriteOps", body)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		after, err = actions.ListAuditEntries(auditCtx, bucket, false)
		Expect(err).To(Succeed())
		Expect(after).To(HaveLen(len(before) + 1))
	})
})
