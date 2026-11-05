//go:build e2e

package business

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
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
	sign := func(method, path string, body any, key string) *signaturepb.SignedHTTPRequest {
		GinkgoHelper()
		var raw []byte
		if original, ok := body.(json.RawMessage); ok {
			raw = original
		} else if body != nil {
			var err error
			raw, err = json.Marshal(body)
			Expect(err).To(Succeed())
		}
		target, err := url.Parse(fmt.Sprintf("http://localhost:%d/v3/%s%s", node.HTTPPort, ledger, path))
		Expect(err).To(Succeed())
		proof := &signaturepb.SignedHTTPRequest{Scheme: target.Scheme, Method: method, RequestTarget: target.RequestURI(), Authority: target.Host, ContentType: "application/json", IdempotencyKey: key, Body: raw}
		Expect(signing.SignHTTP(proof, keyID, private)).To(Succeed())

		return proof
	}
	send := func(method, path string, body any, signed *signaturepb.SignedHTTPRequest, key string) (int, []byte) {
		GinkgoHelper()
		var raw []byte
		var err error
		if original, ok := body.(json.RawMessage); ok {
			raw = original
		} else if body != nil {
			raw, err = json.Marshal(body)
			Expect(err).To(Succeed())
		}
		request, err := http.NewRequestWithContext(GinkgoT().Context(), method, fmt.Sprintf("http://localhost:%d/v3/%s%s", node.HTTPPort, ledger, path), bytes.NewReader(raw))
		Expect(err).To(Succeed())
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		if signed != nil {
			request.Header.Set("Signature-Input", signed.GetSignatureInput())
			request.Header.Set("Signature", signed.GetSignature())
			request.Header.Set("Content-Digest", signed.GetContentDigest())
		}
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
		code, raw := send(http.MethodPost, "/transactions", body("signed"), sign("POST", "/transactions", body("signed"), ""), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})
	It("keeps optional legacy HTTP and existing gRPC signing compatible", func() {
		code, raw := send(http.MethodPost, "/transactions", body("legacy"), nil, "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		code, raw = send(http.MethodPost, "/transactions", body("optional-signed"), sign("POST", "/transactions", body("optional-signed"), ""), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		invalid := sign("POST", "/transactions", body("optional-invalid"), "")
		invalid.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)) + ":"
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
		code, raw := send(http.MethodPost, path+"/metadata", map[string]any{"note": "signed"}, sign("POST", path+"/metadata", map[string]any{"note": "signed"}, ""), "")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		transaction, err := actions.GetTransaction(GinkgoT().Context(), node.Client, ledger, id)
		Expect(err).To(Succeed())
		Expect(transaction.GetTransaction().GetMetadata()["note"].GetStringValue()).To(Equal("signed"))
		code, raw = send(http.MethodDelete, path+"/metadata/note", nil, sign("DELETE", path+"/metadata/note", nil, ""), "")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		transaction, err = actions.GetTransaction(GinkgoT().Context(), node.Client, ledger, id)
		Expect(err).To(Succeed())
		Expect(transaction.GetTransaction().GetMetadata()).NotTo(HaveKey("note"))
		code, raw = send(http.MethodPost, path+"/revert", map[string]any{"metadata": map[string]any{"reason": "signed"}}, sign("POST", path+"/revert", map[string]any{"metadata": map[string]any{"reason": "signed"}}, ""), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(2))
	})
	It("commits ordered atomic bulk once and retains the exact original signature in audit", func() {
		apply(actions.SetSigningConfigAction(true))
		bulk := []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("first"), "ik": "ignored-first"}, map[string]any{"action": "CREATE_TRANSACTION", "data": body("second"), "ik": "ignored-second"}}
		encoded, err := json.Marshal(bulk)
		Expect(err).To(Succeed())
		// Leading/trailing whitespace is signed evidence and must survive audit capture.
		rawBulk := json.RawMessage(append(append([]byte("\n "), encoded...), '\n'))
		envelope := sign("POST", "/bulk?atomic=true", rawBulk, "bulk-identity")
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		code, raw := send(http.MethodPost, "/bulk?atomic=true", rawBulk, envelope, "bulk-identity")
		Expect(code).To(Equal(http.StatusOK), string(raw))
		replayCode, replay := send(http.MethodPost, "/bulk?atomic=true", rawBulk, envelope, "bulk-identity")
		Expect(replayCode).To(Equal(http.StatusOK), string(replay))
		Expect(replay).To(MatchJSON(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(2))
		entries, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		Expect(entries).To(HaveLen(len(before) + 1))
		full, err := node.Client.GetAuditEntry(GinkgoT().Context(), &servicepb.GetAuditEntryRequest{Sequence: entries[len(entries)-1].GetSequence()})
		Expect(err).To(Succeed())
		Expect(proto.Equal(full.GetHttpSignature(), envelope)).To(BeTrue())
		Expect(signing.VerifyHTTP(full.GetHttpSignature(), public)).To(Succeed())
		request, err := http.NewRequestWithContext(GinkgoT().Context(), http.MethodGet,
			fmt.Sprintf("http://localhost:%d/v3/_/audit-entries/%d", node.HTTPPort, full.GetSequence()), nil)
		Expect(err).To(Succeed())
		auditResponse, err := client.Do(request)
		Expect(err).To(Succeed())
		defer func() { Expect(auditResponse.Body.Close()).To(Succeed()) }()
		Expect(auditResponse.StatusCode).To(Equal(http.StatusOK))
		var visible struct {
			Data struct {
				HttpSignature json.RawMessage `json:"httpSignature"`
			} `json:"data"`
		}
		Expect(json.NewDecoder(auditResponse.Body).Decode(&visible)).To(Succeed())
		retained := &signaturepb.SignedHTTPRequest{}
		Expect(protojson.Unmarshal(visible.Data.HttpSignature, retained)).To(Succeed())
		Expect(proto.Equal(envelope, retained)).To(BeTrue())
		Expect(signing.VerifyHTTP(retained, public)).To(Succeed())
		Expect(full.GetItems()).To(HaveLen(2))
		logs, err := actions.ListAllLogs(GinkgoT().Context(), node.Client, ledger)
		Expect(err).To(Succeed())
		Expect(logs).To(HaveLen(2))
		Expect(logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()[0].GetDestination()).To(Equal("first"))
		Expect(logs[1].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()[0].GetDestination()).To(Equal("second"))
	})
	It("rejects signature, body, URL, header, and bulk-order tampering without any committed work", func() {
		apply(actions.SetSigningConfigAction(true))
		before, err := actions.ListAuditEntries(GinkgoT().Context(), node.Client, false)
		Expect(err).To(Succeed())
		valid := sign("POST", "/transactions", body("signed"), "")
		wrong := proto.Clone(valid).(*signaturepb.SignedHTTPRequest)
		_, wrongPrivate, err := actions.GenerateTestKeypair()
		Expect(err).To(Succeed())
		Expect(signing.SignHTTP(wrong, keyID, wrongPrivate)).To(Succeed())
		unknown := proto.Clone(valid).(*signaturepb.SignedHTTPRequest)
		Expect(signing.SignHTTP(unknown, "unknown", private)).To(Succeed())
		invalid := proto.Clone(valid).(*signaturepb.SignedHTTPRequest)
		invalid.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)) + ":"
		payload := proto.Clone(valid).(*signaturepb.SignedHTTPRequest)
		payload.ContentDigest = "sha-256=:" + base64.StdEncoding.EncodeToString(make([]byte, 32)) + ":"
		cases := []struct {
			name, path string
			body       any
			signed     *signaturepb.SignedHTTPRequest
			key        string
			code       int
		}{
			{"invalid signature", "/transactions", body("signed"), invalid, "", 403},
			{"wrong key", "/transactions", body("signed"), wrong, "", 403},
			{"unknown key", "/transactions", body("signed"), unknown, "", 403},
			{"payload bytes", "/transactions", body("signed"), payload, "", 403},
			{"body", "/transactions", body("tampered"), valid, "", 403},
			{"idempotency header", "/transactions", body("signed"), valid, "tampered", 403},
			{"URL target", "/transactions/999/metadata", map[string]any{"note": "signed"}, sign("POST", "/transactions/1/metadata", map[string]any{"note": "signed"}, ""), "", 403},
			{"bulk order", "/bulk?atomic=true&continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("second")}, map[string]any{"action": "CREATE_TRANSACTION", "data": body("first")}}, sign("POST", "/bulk?atomic=true&continueOnFailure=true", []any{map[string]any{"action": "CREATE_TRANSACTION", "data": body("first")}, map[string]any{"action": "CREATE_TRANSACTION", "data": body("second")}}, ""), "", 403},
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
		certs, err := testserver.GenerateTestCerts(GinkgoT().TempDir())
		Expect(err).To(Succeed())
		ctx, nodes, _, leaderID := testutil.SetupMultiNodeCluster(2, func(options *testutil.MultiNodeOptions) {
			options.ExtraInstruments = append(options.ExtraInstruments,
				testserver.WithTLSMode("optional"), testserver.WithTLSCertFile(certs.ServerCertFile),
				testserver.WithTLSKeyFile(certs.ServerKeyFile), testserver.WithTLSCACertFile(certs.CACertFile),
				testserver.WithClusterSecret("http-signing-peer-secret"))
		})
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
		_, err = node.Client.Apply(GinkgoT().Context(), servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil), actions.RegisterSigningKeyAction(keyID, public)))
		Expect(err).To(Succeed())
		apply(actions.SetSigningConfigAction(true))
		leader := node
		node = follower
		code, raw := send(http.MethodPost, "/transactions", body("follower"), sign("POST", "/transactions", body("follower"), ""), "")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		invalid := sign("POST", "/transactions", body("invalid-follower"), "")
		invalid.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)) + ":"
		code, raw = send(http.MethodPost, "/transactions", body("invalid-follower"), invalid, "")
		Expect(code).To(Equal(http.StatusForbidden), string(raw))
		code, raw = send(http.MethodPost, "/transactions", body("unsigned-follower"), nil, "")
		Expect(code).To(Equal(http.StatusUnauthorized), string(raw))
		transactions, err := actions.ListAllTransactions(GinkgoT().Context(), leader.Client, ledger)
		Expect(err).To(Succeed())
		Expect(transactions).To(HaveLen(1))
	})

})
