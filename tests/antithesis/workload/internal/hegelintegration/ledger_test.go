package hegelintegration

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	workload "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// The server is shared; business state and idempotency keys are fresh on EVERY
// callback, including discarded examples, shrink attempts and final blob replay.
// Hegel v0.9.27 prints and automatically replays failure blobs. Its public Go
// API has no option to supply a blob (TestCaseFromBlob is an internal API).
func TestSharedGenerators(t *testing.T) {
	t.Parallel()
	client := startLedger(t)
	var callbacks, creates, replays, reverts int
	defer func() {
		t.Logf("callbacks=%d creates=%d idempotencyReplays=%d reverts=%d (includes shrink/replay)", callbacks, creates, replays, reverts)
	}()
	hegel.Test(t, func(ht *hegel.T) {
		callbacks++
		ledger := fmt.Sprintf("shared-hegel-%d", callbacks)
		ht.Note("fresh logical ledger: " + ledger)
		ctx, cleanup := freshLedger(t, ht, client, ledger)
		defer cleanup()
		src := generation.NewSource(ht)
		e := episode{ht: ht, ctx: ctx, client: client, ledger: ledger, model: make(oracle)}
		// Choices operate on the actual created transaction stack: dependent
		// reverts cannot target a nonexistent or already reverted transaction.
		steps := 1 + src.Intn(6)
		for range steps {
			choice := generation.Choice(src, []string{"create", "create", "replay", "revert"})
			if len(e.pending) == 0 {
				choice = "create"
			}
			ht.Note("operation: " + choice)
			switch choice {
			case "create":
				e.create(workload.GeneratedPostings(src))
				creates++
				replays++
			case "replay":
				e.replay(e.pending[len(e.pending)-1])
				replays++
			case "revert":
				e.revertLast(false)
				reverts++
			}
		}
		for len(e.pending) > 0 {
			e.revertLast(false)
			reverts++
		}
		e.checkFinal()
	}, localOptions(100)...)
}

// Deterministic examples establish successful coverage without probabilistic
// assertions about the values Hegel happens to draw. No generated domain is
// filtered or clamped to make these examples pass.
func TestBoundaryTransactions(t *testing.T) {
	t.Parallel()
	client := startLedger(t)
	for _, amount := range []uint64{0, 1, 1<<63 + 1, math.MaxUint64} {
		for _, self := range []bool{false, true} {
			for sourceIndex, source := range []string{"world", "users:0"} {
				t.Run(fmt.Sprintf("amount=%d/self=%t/source=%s", amount, self, source), func(t *testing.T) {
					callback := 0
					hegel.Test(t, func(ht *hegel.T) {
						callback++
						ledger := fmt.Sprintf("boundary-%d-%t-%d-%d", amount, self, sourceIndex, callback)
						ctx, cleanup := freshLedger(t, ht, client, ledger)
						defer cleanup()
						e := episode{ht: ht, ctx: ctx, client: client, ledger: ledger, model: make(oracle)}
						dst := "users:1"
						if self {
							dst = source
						}
						e.create([]*commonpb.Posting{commonpb.NewPosting(source, dst, "USD/2", new(big.Int).SetUint64(amount))})
						e.revertLast(false)
						e.checkFinal()
					}, localOptions(1)...)
				})
			}
		}
	}
}

// Run HEGEL_INTEGRATION_OMIT_REVERT=1 go test -run TestMissingRevertSensitivity
// -count=1 -v . to deliberately FAIL. The fault skips a real revert RPC while
// the independent accounting oracle still expects it. The amount comes from
// the same full-uint64 generator, so Hegel can shrink it to the boundary 1.
func TestMissingRevertSensitivity(t *testing.T) {
	if os.Getenv("HEGEL_INTEGRATION_OMIT_REVERT") != "1" {
		t.Skip("opt-in deliberate missing-revert fault")
	}
	client := startLedger(t)
	callbacks := 0
	defer func() { t.Logf("fresh-state sensitivity callbacks=%d", callbacks) }()
	hegel.Test(t, func(ht *hegel.T) {
		callbacks++
		ledger := fmt.Sprintf("sensitivity-%d", callbacks)
		ht.Note("fresh logical ledger: " + ledger)
		ctx, cleanup := freshLedger(t, ht, client, ledger)
		defer cleanup()
		src := generation.NewSource(ht)
		e := episode{ht: ht, ctx: ctx, client: client, ledger: ledger, model: make(oracle)}
		for range 1 + src.Intn(6) {
			e.create([]*commonpb.Posting{commonpb.NewPosting("world", "users:0", "USD/2", workload.GeneratedBigInt(src))})
		}
		for len(e.pending) > 0 {
			e.revertLast(true)
		}
	}, localOptions(100)...)
}

func localOptions(cases int) []hegel.Option {
	return []hegel.Option{
		hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault),
		hegel.WithSeed(42), hegel.WithDatabase(""), hegel.WithTestCases(cases),
		hegel.WithReproductionBlob(true), hegel.WithReportMultipleFailures(false),
		// RPC/ReadIndex latency is expected. Keep all other health checks.
		hegel.SuppressHealthCheck(hegel.TooSlow),
	}
}

func freshLedger(t *testing.T, ht *hegel.T, client servicepb.BucketServiceClient, ledger string) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	// Case-scoped cleanup must run during shrinking too, not at test end.
	apply(ht, ctx, client, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))

	return ctx, func() {
		defer cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cleanupCancel()
		if _, err := client.Apply(cleanupCtx, servicepb.UnsignedApplyRequest("", actions.DeleteLedgerAction(ledger))); err != nil {
			t.Errorf("delete ledger %s: %v", ledger, err)
		}
	}
}

type committed struct {
	id       uint64
	request  *servicepb.ApplyRequest
	response *servicepb.ApplyResponse
	postings []intent
}

type episode struct {
	ht       *hegel.T
	ctx      context.Context
	client   servicepb.BucketServiceClient
	ledger   string
	model    oracle
	pending  []committed
	sequence int
}

func (e *episode) create(ps []*commonpb.Posting) {
	// Capture expected values BEFORE the RPC; responses only supply identity.
	inputs := capture(ps)
	e.sequence++
	req := actions.WithIdempotencyKey(fmt.Sprintf("%s-%d", e.ledger, e.sequence), actions.CreateForceTransactionAction(e.ledger, ps, nil))
	response := apply(e.ht, e.ctx, e.client, req)
	if len(response.GetLogs()) != 1 {
		e.ht.Fatalf("create returned %d logs", len(response.GetLogs()))
	}
	tx := response.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
	if tx == nil {
		e.ht.Fatal("create returned no transaction")
	}
	e.model.add(inputs, false)
	e.check()
	c := committed{id: tx.GetId(), request: req, response: response, postings: inputs}
	e.pending = append(e.pending, c)
	// Every successful create has a mandatory idempotency check, independently
	// of the additional replays selected by the generated operation sequence.
	e.replay(c)
}

func (e *episode) replay(c committed) {
	if got := apply(e.ht, e.ctx, e.client, c.request); !proto.Equal(c.response, got) {
		e.ht.Fatal("idempotency replay changed the response")
	}
	e.check()
}

func (e *episode) revertLast(omit bool) {
	i := len(e.pending) - 1
	c := e.pending[i]
	// Leave zero-amount examples correct so the sensitivity failure specifically
	// proves the accounting assertion, rather than just the reverted flag.
	if !omit || !hasValue(c.postings) {
		apply(e.ht, e.ctx, e.client, servicepb.UnsignedApplyRequest("", actions.RevertTransactionAction(e.ledger, c.id, true, false, nil)))
	}
	e.model.add(c.postings, true)
	e.check()
	read, err := e.client.GetTransaction(e.ctx, &servicepb.GetTransactionRequest{Ledger: e.ledger, TransactionId: c.id})
	if err != nil || !read.GetTransaction().GetReverted() {
		e.ht.Fatalf("transaction %d not marked reverted: %v", c.id, err)
	}
	e.pending = e.pending[:i]
}

func apply(ht *hegel.T, ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.ApplyRequest) *servicepb.ApplyResponse {
	response, err := client.Apply(ctx, req)
	if err != nil {
		ht.Fatalf("Apply failed: %v", err)
	}

	return response
}
