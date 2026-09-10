//go:build e2e

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// restOutcome is the client-visible part of a REST error: what a caller
// branches on.
type restOutcome struct {
	StatusCode   int
	ErrorCode    string
	ErrorMessage string
}

// EN-1636. Every REST write is routed to the Raft leader, so a request that
// lands on a follower crosses a gRPC hop before the FSM sees it. The leader's
// typed business error used to lose its identity on that hop and reach the
// caller as 500 INTERNAL_ERROR with a correlation ID, while the same request
// against the leader returned the correct 4xx and errorCode. Roughly (N-1)/N
// of requests were affected, depending on which node the load balancer picked.
//
// This is the first e2e suite that combines a multi-node cluster with the HTTP
// surface: every other HTTP suite is single-node (so it only ever exercises the
// leader-local path, where the typed error stays in-process), and every
// multi-node write in the tree goes over gRPC, where the status and its
// ErrorInfo already survived the hop.
//
// The specs assert node-to-node parity rather than only the expected value.
// Parity is the actual contract — a client must not care which node answered —
// and asserting it catches a regression that shifts both paths consistently but
// wrongly, which a leader-only expectation would miss.
//
// Only writes are covered here, deliberately. Reads are served locally behind a
// ReadIndex barrier and forward on syncing or an in-flight leadership change,
// which are not deterministic routes in a healthy cluster; the HTTP
// surface exposes no consistency header to force it. The forwarded-read and
// list-stream paths run through the same seam and are covered at the unit level
// by internal/adapter/grpcerr (TestConn_StreamErrorIsReconstructed).
var _ = Describe("REST error parity across cluster nodes (EN-1636)", Ordered, func() {
	const (
		countInstances = 3
		ledgerName     = "err-parity"
		deletedLedger  = "err-parity-deleted"
	)

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(countInstances)

		_, err := servers[0].Client.Apply(ctx,
			servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = servers[0].Client.Apply(ctx,
			servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(deletedLedger, nil)))
		Expect(err).To(Succeed())
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	// post issues the request against one node and returns what the caller sees.
	post := func(node *testutil.ServiceWithClient, path, body string) restOutcome {
		GinkgoHelper()

		url := fmt.Sprintf("http://localhost:%d/v3%s", node.HTTPPort, path)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()

		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())

		var decoded struct {
			ErrorCode    string `json:"errorCode"`
			ErrorMessage string `json:"errorMessage"`
		}
		Expect(json.Unmarshal(raw, &decoded)).To(Succeed(), "unparseable body from node %d: %s", node.NodeID, raw)

		return restOutcome{
			StatusCode:   resp.StatusCode,
			ErrorCode:    decoded.ErrorCode,
			ErrorMessage: decoded.ErrorMessage,
		}
	}

	// splitByRole separates the leader from the followers. The followers are the
	// nodes whose responses used to degrade.
	splitByRole := func() (*testutil.ServiceWithClient, []*testutil.ServiceWithClient) {
		GinkgoHelper()

		var (
			leader    *testutil.ServiceWithClient
			followers []*testutil.ServiceWithClient
		)

		for _, s := range servers {
			if uint64(s.NodeID) == *leaderID {
				leader = s
			} else {
				followers = append(followers, s)
			}
		}

		Expect(leader).NotTo(BeNil(), "no server matched the leader id %d", *leaderID)
		Expect(followers).To(HaveLen(countInstances-1),
			"the parity assertion is vacuous without at least one follower")

		return leader, followers
	}

	// expectParity sends the same rejected request to the leader and to every
	// follower and asserts all public REST error fields are identical. REST does
	// not expose structured metadata; the gRPC spec below checks that separately.
	expectParity := func(path, body string, wantStatus int, wantCode string) {
		GinkgoHelper()

		leader, followers := splitByRole()

		// The write is rejected on every attempt (it never commits), so
		// re-sending it to each node is safe and each node answers independently.
		onLeader := post(leader, path, body)

		Expect(onLeader.StatusCode).To(Equal(wantStatus), "leader body: %s", onLeader.ErrorMessage)
		Expect(onLeader.ErrorCode).To(Equal(wantCode))
		Expect(onLeader.ErrorMessage).NotTo(BeEmpty())

		for _, follower := range followers {
			onFollower := post(follower, path, body)

			Expect(onFollower.StatusCode).To(Equal(wantStatus),
				"node %d answered %d where the leader answered %d; body: %s",
				follower.NodeID, onFollower.StatusCode, onLeader.StatusCode, onFollower.ErrorMessage)
			Expect(onFollower.ErrorCode).To(Equal(wantCode),
				"node %d answered errorCode %q where the leader answered %q",
				follower.NodeID, onFollower.ErrorCode, onLeader.ErrorCode)
			Expect(onFollower.ErrorMessage).To(Equal(onLeader.ErrorMessage),
				"node %d changed the leader's safe error message: got %q, want %q",
				follower.NodeID, onFollower.ErrorMessage, onLeader.ErrorMessage)

			// The sanitizer's fingerprints. Asserted separately from the
			// equality above so a failure says which contract broke.
			Expect(onFollower.ErrorCode).NotTo(Equal("INTERNAL_ERROR"))
			Expect(onFollower.ErrorMessage).NotTo(ContainSubstring("correlation ID"))
			Expect(onFollower.ErrorMessage).NotTo(ContainSubstring("gRPC call failed"))
		}
	}

	It("returns the same 400 and errorCode on every node for an undeclared metadata field", func() {
		// The ticket's reproduction: an index on a metadata field that was never
		// declared through SetMetadataFieldType. KindPrecondition, so 400.
		expectParity(
			"/"+ledgerName+"/indexes",
			`{"id":"metadata:TARGET_TYPE_ACCOUNT:never-declared"}`,
			http.StatusBadRequest,
			"METADATA_FIELD_NOT_IN_SCHEMA",
		)
	})

	It("returns the same 409 and errorCode on every node for an existing ledger", func() {
		// KindAlreadyExists travels as codes.AlreadyExists, a code no other kind
		// shares, so this row passes under any code-derived reconstruction. It is
		// here for the errorCode half of the contract: before the fix a follower
		// dropped LEDGER_ALREADY_EXISTS entirely.
		expectParity("/"+ledgerName, `{}`, http.StatusConflict, "LEDGER_ALREADY_EXISTS")
	})

	It("preserves the gRPC reason, safe message, and metadata through a follower (EN-1980)", func() {
		leader, followers := splitByRole()
		apply := func(node *testutil.ServiceWithClient) *status.Status {
			GinkgoHelper()
			_, err := node.Client.Apply(ctx,
				servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			return st
		}

		onLeader := apply(leader)
		Expect(onLeader.Code()).To(Equal(codes.AlreadyExists))
		Expect(onLeader.Message()).To(Equal("ledger already exists: " + ledgerName))
		Expect(onLeader.Details()).To(HaveLen(1))
		info, ok := onLeader.Details()[0].(*errdetails.ErrorInfo)
		Expect(ok).To(BeTrue())
		Expect(info.Reason).To(Equal("LEDGER_ALREADY_EXISTS"))
		Expect(info.Domain).To(Equal("ledger"))
		Expect(info.Metadata).To(Equal(map[string]string{"name": ledgerName}))

		for _, follower := range followers {
			onFollower := apply(follower)
			Expect(proto.Equal(onFollower.Proto(), onLeader.Proto())).To(BeTrue(),
				"node %d changed the leader's gRPC error: got %v, want %v",
				follower.NodeID, onFollower.Proto(), onLeader.Proto())
		}
	})

	It("returns the same 409 and errorCode on every node for a write to a deleted ledger", func() {
		// The collapse case, and the reason this spec is not redundant with the
		// row above. kindToGRPCCode sends both KindConflict and KindPrecondition
		// as codes.FailedPrecondition, so reconstructing the kind from the status
		// code alone answers 400 here while the leader answers 409. Only the
		// reason (LEDGER_DELETED) distinguishes them.
		leader, _ := splitByRole()

		url := fmt.Sprintf("http://localhost:%d/v3/%s", leader.HTTPPort, deletedLedger)
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		Expect(err).To(Succeed())

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(BeNumerically("<", 300), "deleting the fixture ledger must succeed")

		// The deletion must be visible on every node before the parity check, or
		// a follower could answer LEDGER_ALREADY_EXISTS from a stale read.
		Eventually(func(g Gomega) {
			for _, s := range servers {
				out := post(s, "/"+deletedLedger, `{}`)
				g.Expect(out.ErrorCode).To(Equal("LEDGER_DELETED"),
					"node %d has not observed the deletion yet", s.NodeID)
			}
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		expectParity("/"+deletedLedger, `{}`, http.StatusConflict, "LEDGER_DELETED")
	})

	It("returns the same per-element reason and safe message on every node for a failing bulk element", func() {
		// Bulk has its own error mapper (perElementStatus / bulkErrorCode), which
		// dispatches on the same Describable contract. Before the fix it lacked
		// even the InvalidArgument consolation the single-request path had, so
		// every kind surfaced as errorCode "ERROR".
		body := `[{"action":"CREATE_TRANSACTION","data":{"postings":` +
			`[{"source":"no-such-account:a","destination":"bank","amount":100,"asset":"USD/2"}]}}]`

		leader, followers := splitByRole()

		elementOutcome := func(node *testutil.ServiceWithClient) restOutcome {
			GinkgoHelper()

			url := fmt.Sprintf("http://localhost:%d/v3/%s/bulk", node.HTTPPort, ledgerName)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
			Expect(err).To(Succeed())
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			defer func() { _ = resp.Body.Close() }()

			raw, err := io.ReadAll(resp.Body)
			Expect(err).To(Succeed())

			var decoded struct {
				ErrorCode string `json:"errorCode"`
				Data      []struct {
					ErrorCode        string `json:"errorCode"`
					ErrorDescription string `json:"errorDescription"`
					ResponseType     string `json:"responseType"`
				} `json:"data"`
			}
			Expect(json.Unmarshal(raw, &decoded)).To(Succeed(), "unparseable bulk body: %s", raw)

			// The failure must be reported per element, inside data[]. A
			// request-level rejection — a parse failure, say, which serveBulk
			// answers with a top-level "VALIDATION" before any element reaches
			// admission — never crosses the forwarding seam this spec exists to
			// cover, yet is non-empty and identical on every node. Asserting the
			// element shape here is what stops such a rejection from satisfying
			// the spec: a later change to bulk parsing or admission fails it
			// instead of silently making it test nothing.
			Expect(decoded.Data).To(HaveLen(1),
				"the rejection must be per-element, not request-level; top-level errorCode was %q, body: %s",
				decoded.ErrorCode, raw)
			Expect(decoded.ErrorCode).To(BeEmpty())
			Expect(decoded.Data[0].ResponseType).To(Equal("ERROR"))

			return restOutcome{
				StatusCode:   resp.StatusCode,
				ErrorCode:    decoded.Data[0].ErrorCode,
				ErrorMessage: decoded.Data[0].ErrorDescription,
			}
		}

		// The posting's source is an account with no balance, so admission
		// rejects the single element with INSUFFICIENT_FUNDS (KindPrecondition,
		// 400). Pinning the expected code rather than only "non-empty and equal"
		// means the spec fails if the element stops reaching the FSM at all.
		onLeader := elementOutcome(leader)
		Expect(onLeader.ErrorCode).To(Equal("INSUFFICIENT_FUNDS"),
			"the leader must name the business reason of the element rejection")
		Expect(onLeader.StatusCode).To(Equal(http.StatusBadRequest))
		Expect(onLeader.ErrorMessage).NotTo(BeEmpty())

		for _, follower := range followers {
			Expect(elementOutcome(follower)).To(Equal(onLeader),
				"node %d reported a different per-element error than the leader", follower.NodeID)
		}
	})
})
