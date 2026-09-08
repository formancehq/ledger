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
// ReadIndex barrier and forward to the leader only while a node is syncing,
// which is not reachable deterministically in a healthy cluster; the HTTP
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
	// follower and asserts they answer identically, with the expected outcome.
	expectParity := func(path, body string, wantStatus int, wantCode string) {
		GinkgoHelper()

		leader, followers := splitByRole()

		// The write is rejected on every attempt (it never commits), so
		// re-sending it to each node is safe and each node answers independently.
		onLeader := post(leader, path, body)

		Expect(onLeader.StatusCode).To(Equal(wantStatus), "leader body: %s", onLeader.ErrorMessage)
		Expect(onLeader.ErrorCode).To(Equal(wantCode))

		for _, follower := range followers {
			onFollower := post(follower, path, body)

			Expect(onFollower.StatusCode).To(Equal(wantStatus),
				"node %d answered %d where the leader answered %d; body: %s",
				follower.NodeID, onFollower.StatusCode, onLeader.StatusCode, onFollower.ErrorMessage)
			Expect(onFollower.ErrorCode).To(Equal(wantCode),
				"node %d answered errorCode %q where the leader answered %q",
				follower.NodeID, onFollower.ErrorCode, onLeader.ErrorCode)

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

	It("returns the same per-element errorCode on every node for a failing bulk element", func() {
		// Bulk has its own error mapper (perElementStatus / bulkErrorCode), which
		// dispatches on the same Describable contract. Before the fix it lacked
		// even the InvalidArgument consolation the single-request path had, so
		// every kind surfaced as errorCode "ERROR".
		body := `[{"action":"CREATE_TRANSACTION","data":{"postings":` +
			`[{"source":"no-such-account:a","destination":"bank","amount":100,"asset":"USD/2"}]}}]`

		leader, followers := splitByRole()

		elementCode := func(node *testutil.ServiceWithClient) string {
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
					ErrorCode string `json:"errorCode"`
				} `json:"data"`
			}
			Expect(json.Unmarshal(raw, &decoded)).To(Succeed(), "unparseable bulk body: %s", raw)

			// A per-element failure is reported inside data[]; a request-level
			// failure sets the top-level errorCode. Return whichever fired so a
			// mismatch between nodes is visible either way.
			if len(decoded.Data) > 0 && decoded.Data[0].ErrorCode != "" {
				return decoded.Data[0].ErrorCode
			}

			return decoded.ErrorCode
		}

		onLeader := elementCode(leader)
		Expect(onLeader).NotTo(BeEmpty(), "the element must fail for this spec to mean anything")
		Expect(onLeader).NotTo(Equal("ERROR"), "the leader must name the business reason")

		for _, follower := range followers {
			Expect(elementCode(follower)).To(Equal(onLeader),
				"node %d reported a different per-element errorCode than the leader", follower.NodeID)
		}
	})
})
