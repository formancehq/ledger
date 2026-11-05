//go:build e2e

package business

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("HTTP request signing", func() {
	const ledger = "http-signing"
	const keyID = "http-signing-root"
	var node *testutil.ServiceWithClient
	var private ed25519.PrivateKey
	var public ed25519.PublicKey
	client := &http.Client{Timeout: 10 * time.Second}
	apply := func(requests ...*servicepb.Request) *servicepb.ApplyResponse {
		GinkgoHelper()
		signed, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: requests}, keyID, private)
		Expect(err).To(Succeed())
		response, err := node.Client.Apply(GinkgoT().Context(), signed)
		Expect(err).To(Succeed())

		return response
	}
	sign := func(key string, requests ...*servicepb.Request) *signaturepb.SignedApplyBatch {
		GinkgoHelper()
		signed, err := actions.SignBatch(&servicepb.ApplyBatch{Requests: requests, IdempotencyKey: key}, keyID, private)
		Expect(err).To(Succeed())

		return signed.GetSigned()
	}
	send := func(method, path string, body any, signed *signaturepb.SignedApplyBatch, key string) (int, []byte) {
		GinkgoHelper()
		contentType := "application/json"
		if signed != nil {
			contentType = "application/vnd.formance.signed-request+json"
			body = map[string]any{"request": body, "signedBatch": map[string]any{"keyId": signed.GetKeyId(), "signature": signed.GetSignature(), "payload": signed.GetPayload()}}
		}
		raw, err := json.Marshal(body)
		Expect(err).To(Succeed())
		request, err := http.NewRequestWithContext(GinkgoT().Context(), method, fmt.Sprintf("http://localhost:%d/v3/%s%s", node.HTTPPort, ledger, path), bytes.NewReader(raw))
		Expect(err).To(Succeed())
		request.Header.Set("Content-Type", contentType)
		request.Header.Set("Idempotency-Key", key)
		response, err := client.Do(request)
		Expect(err).To(Succeed())
		defer func() { Expect(response.Body.Close()).To(Succeed()) }()
		raw, err = io.ReadAll(response.Body)
		Expect(err).To(Succeed())

		return response.StatusCode, raw
	}
	create := func(destination string) *servicepb.Request {
		return actions.CreateTransactionAction(ledger, []*commonpb.Posting{actions.NewPosting("world", destination, big.NewInt(100), "USD")}, nil, nil)
	}
	body := func(destination string) any {
		return map[string]any{"postings": []any{map[string]any{"source": "world", "destination": destination, "amount": 100, "asset": "USD"}}}
	}
	BeforeEach(func() {
		var err error
		_, node = testutil.SetupSingleNode()
		public, private, err = actions.GenerateTestKeypair()
		Expect(err).To(Succeed())
		_, err = node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil), actions.RegisterSigningKeyAction(keyID, public)))
		Expect(err).To(Succeed())
	})
	It("commits a signed HTTP transaction when admission requires signatures", func() {
		apply(actions.SetSigningConfigAction(true))
		code, raw := send(http.MethodPost, "/transactions", body("signed"), sign("", create("signed")), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})
	It("keeps optional legacy HTTP and existing gRPC signing compatible", func() {
		code, raw := send(http.MethodPost, "/transactions", body("legacy"), nil, "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		code, raw = send(http.MethodPost, "/transactions", body("optional-signed"), sign("", create("optional-signed")), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		invalid := sign("", create("optional-invalid"))
		invalid.Signature[0] ^= 1
		code, raw = send(http.MethodPost, "/transactions", body("optional-invalid"), invalid, "")
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		apply(actions.SetSigningConfigAction(true))
		apply(create("grpc"))
		_, err := node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", create("unsigned-grpc")))
		Expect(status.Code(err)).To(Equal(codes.Unauthenticated))
		code, raw = send(http.MethodPost, "/transactions", body("unsigned-http"), nil, "")
		Expect(code).To(Equal(http.StatusUnauthorized), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(3))
	})
	It("signs metadata set, metadata deletion, and revert using URL-derived targets", func() {
		response := apply(create("metadata"))
		id, ok := actions.GetCreatedTransactionID(response)
		Expect(ok).To(BeTrue())
		apply(actions.SetSigningConfigAction(true))
		path := fmt.Sprintf("/transactions/%d", id)
		set := actions.SaveTransactionMetadataAction(ledger, id, map[string]string{"note": "signed"})
		code, raw := send(http.MethodPost, path+"/metadata", map[string]any{"note": "signed"}, sign("", set), "")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		transaction, err := actions.GetTransaction(GinkgoT().Context(), node.Client, ledger, id)
		Expect(err).To(Succeed())
		Expect(transaction.GetTransaction().GetMetadata()["note"].GetStringValue()).To(Equal("signed"))
		deletion := actions.DeleteTransactionMetadataAction(ledger, id, "note")
		code, raw = send(http.MethodDelete, path+"/metadata/note", nil, sign("", deletion), "")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		transaction, err = actions.GetTransaction(GinkgoT().Context(), node.Client, ledger, id)
		Expect(err).To(Succeed())
		Expect(transaction.GetTransaction().GetMetadata()).NotTo(HaveKey("note"))
		revert := actions.RevertTransactionAction(ledger, id, false, false, map[string]string{"reason": "signed"})
		code, raw = send(http.MethodPost, path+"/revert", map[string]any{"metadata": map[string]any{"reason": "signed"}}, sign("", revert), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(2))
	})
	It("replays the same signed batch across HTTP and gRPC and retains its original signature in audit", func() {
		apply(actions.SetSigningConfigAction(true))
		requests := []*servicepb.Request{create("first"), create("second")}
		envelope := sign("bulk-identity", requests...)
		// Move the idempotency field before requests: equivalent protobuf wire
		// bytes must remain intact through admission and the committed audit entry.
		payload := protowire.AppendString(protowire.AppendTag(nil, 2, protowire.BytesType), "bulk-identity")
		for _, request := range requests {
			encoded, err := request.MarshalVT()
			Expect(err).To(Succeed())
			payload = protowire.AppendBytes(protowire.AppendTag(payload, 1, protowire.BytesType), encoded)
		}
		envelope.Payload = payload
		envelope.Signature = ed25519.Sign(private, payload)
		bulk := []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("first"), "ik": "ignored-first"}, map[string]any{"action": "CREATE_TRANSACTION", "data": body("second"), "ik": "ignored-second"}}
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		code, raw := send(http.MethodPost, "/bulk?atomic=true", bulk, envelope, "bulk-identity")
		Expect(code).To(Equal(http.StatusOK), string(raw))
		replayCode, replay := send(http.MethodPost, "/bulk?atomic=true", bulk, envelope, "bulk-identity")
		Expect(replayCode).To(Equal(http.StatusOK), string(replay))
		Expect(replay).To(MatchJSON(raw))
		// Both transports submit the very same envelope to admission. Switching
		// transport must replay the outcome without another audit entry or log.
		grpcReplay, err := node.Client.Apply(GinkgoT().Context(), servicepb.SignedApplyRequest(envelope))
		Expect(err).To(Succeed())
		Expect(grpcReplay.GetLogs()).To(HaveLen(2))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(2))
		entries, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		Expect(entries).To(HaveLen(len(before) + 1))
		full, err := node.Client.GetAuditEntry(GinkgoT().Context(), &servicepb.GetAuditEntryRequest{Sequence: entries[len(entries)-1].GetSequence()})
		Expect(err).To(Succeed())
		Expect(full.GetSignature().GetPayload()).To(Equal(envelope.GetPayload()))
		Expect(full.GetSignature().GetSignature()).To(Equal(envelope.GetSignature()))
		Expect(ed25519.Verify(public, full.GetSignature().GetPayload(), full.GetSignature().GetSignature())).To(BeTrue())
		Expect(full.GetItems()).To(HaveLen(2))
		logs, err := actions.ListAllLogs(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(logs).To(HaveLen(2))
		Expect(grpcReplay.GetLogs()[0].GetSequence()).To(Equal(logs[0].GetSequence()))
		Expect(grpcReplay.GetLogs()[1].GetSequence()).To(Equal(logs[1].GetSequence()))
		Expect(logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()[0].GetDestination()).To(Equal("first"))
		Expect(logs[1].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()[0].GetDestination()).To(Equal("second"))
	})
	It("rejects signature, body, URL, header, and bulk-order tampering without any committed work", func() {
		apply(actions.SetSigningConfigAction(true))
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		valid := sign("", create("signed"))
		wrong := valid.CloneVT()
		_, wrongPrivate, err := actions.GenerateTestKeypair()
		Expect(err).To(Succeed())
		wrong.Signature = ed25519.Sign(wrongPrivate, wrong.GetPayload())
		unknown := valid.CloneVT()
		unknown.KeyId = "unknown"
		invalid := valid.CloneVT()
		invalid.Signature[0] ^= 1
		payload := valid.CloneVT()
		payload.Payload[0] ^= 1
		cases := []struct {
			name, path string
			body       any
			signed     *signaturepb.SignedApplyBatch
			key        string
			code       int
		}{
			{"invalid signature", "/transactions", body("signed"), invalid, "", 403},
			{"wrong key", "/transactions", body("signed"), wrong, "", 403},
			{"unknown key", "/transactions", body("signed"), unknown, "", 403},
			{"payload bytes", "/transactions", body("signed"), payload, "", 403},
			{"body", "/transactions", body("tampered"), valid, "", 403},
			{"idempotency header", "/transactions", body("signed"), valid, "tampered", 403},
			{"URL target", "/transactions/999/metadata", map[string]any{"note": "signed"}, sign("", actions.SaveTransactionMetadataAction(ledger, 1, map[string]string{"note": "signed"})), "", 403},
			{"bulk order", "/bulk?atomic=true&continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("second")}, map[string]any{"action": "CREATE_TRANSACTION", "data": body("first")}}, sign("", create("first"), create("second")), "", 403},
			{"sequential signed bulk", "/bulk?continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("signed")}}, valid, "", 400},
			{"unsigned bulk auth", "/bulk?atomic=true&continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("signed")}}, nil, "", 401},
			{"invalid bulk auth", "/bulk?atomic=true&continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("signed")}}, invalid, "", 403},
		}
		for _, test := range cases {
			By(test.name)
			code, raw := send(http.MethodPost, test.path, test.body, test.signed, test.key)
			Expect(code).To(Equal(test.code), test.name+": "+string(raw))
		}
		apply(actions.RevokeSigningKeyAction(keyID, false))
		code, raw := send(http.MethodPost, "/transactions", body("signed"), valid, "")
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(BeEmpty())
		after, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		// Only the explicit revocation was committed after the baseline.
		Expect(after).To(HaveLen(len(before) + 1))
	})

	It("preserves signature rejections forwarded from a follower to its leader", func() {
		ctx, nodes, _, leaderID := testutil.SetupMultiNodeCluster(2)
		DeferCleanup(func() {
			testutil.StopServers(ctx, nodes)
		})
		var follower *testutil.ServiceWithClient
		for _, candidate := range nodes {
			if candidate.NodeID == *leaderID {
				node = candidate
			} else {
				follower = candidate
			}
		}
		Expect(follower).NotTo(BeNil())
		_, err := node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil), actions.RegisterSigningKeyAction(keyID, public)))
		Expect(err).To(Succeed())
		apply(actions.SetSigningConfigAction(true))
		leader := node
		node = follower
		code, raw := send(http.MethodPost, "/transactions", body("follower"), sign("", create("follower")), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		invalid := sign("", create("invalid-follower"))
		invalid.Signature[0] ^= 1
		code, raw = send(http.MethodPost, "/transactions", body("invalid-follower"), invalid, "")
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		code, raw = send(http.MethodPost, "/transactions", body("unsigned-follower"), nil, "")
		Expect(code).To(Equal(http.StatusUnauthorized), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), leader.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})

})
