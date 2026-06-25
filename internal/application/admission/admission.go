package admission

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/receipt"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/pkg/vtmarshal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

var tracer = otel.Tracer("admission")

type Proposer interface {
	Propose(context.Context, *node.Proposal) (*futures.Future[state.ApplyResult], error)
	InitialIndex() uint64
}

// Admission handles the admission of orders into the Raft cluster.
// It is responsible for preloading volumes and proposing commands.
type Admission struct {
	store              *dal.Store
	logger             logging.Logger
	proposer           Proposer
	writeGate          health.WriteGate
	keyStore           *keystore.KeyStore
	sharedState        *state.SharedState
	receiptSigner      *receipt.Signer
	builder            *plan.Builder
	attrs              *attributes.Attributes
	numscriptCache     *numscript.NumscriptCache
	coldStorageEnabled bool
	waitLeaderReady    func(context.Context) error

	// Metrics (noop when metricsEnabled is false)
	metricsEnabled                 bool
	commandDurationHistogram       metric.Int64Histogram
	commandSizeHistogram           metric.Int64Histogram
	proposeQueueLoadHistogram      metric.Int64Histogram
	proposeQueueInflight           atomic.Int32
	proposeQueueFullCounter        metric.Float64Counter
	proposeDurationHistogram       metric.Int64Histogram
	fsmFutureWaitHistogram         metric.Int64Histogram
	proposalGuardDurationHistogram metric.Int64Histogram
	proposalGuardRebuildCounter    metric.Int64Counter
	preloadDurationHistogram       metric.Int64Histogram
	preloadCounter                 metric.Int64Counter
	preloadKeysNeededCounter       metric.Int64Counter
	preloadCacheHitsCounter        metric.Int64Counter
}

// NewAdmission creates a new Admission handler.
// WithMetrics enables admission metrics. By default metrics are disabled
// (noop) to avoid contention under high concurrency.
func WithMetrics() func(*Admission) {
	return func(a *Admission) {
		a.metricsEnabled = true
	}
}

// WithReceiptSigner enables receipt-based revert by providing a receipt signer.
func WithReceiptSigner(signer *receipt.Signer) func(*Admission) {
	return func(a *Admission) {
		a.receiptSigner = signer
	}
}

// WithColdStorageEnabled marks cold storage as available, allowing archive operations.
func WithColdStorageEnabled() func(*Admission) {
	return func(a *Admission) {
		a.coldStorageEnabled = true
	}
}

func NewAdmission(
	store *dal.Store,
	logger logging.Logger,
	proposer Proposer,
	builder *plan.Builder,
	meterProvider metric.MeterProvider,
	writeGate health.WriteGate,
	keyStore *keystore.KeyStore,
	sharedState *state.SharedState,
	attrs *attributes.Attributes,
	numscriptCache *numscript.NumscriptCache,
	waitLeaderReady func(context.Context) error,
	opts ...func(*Admission),
) *Admission {
	a := &Admission{
		store:           store,
		logger:          logger,
		proposer:        proposer,
		builder:         builder,
		writeGate:       writeGate,
		keyStore:        keyStore,
		sharedState:     sharedState,
		attrs:           attrs,
		numscriptCache:  numscriptCache,
		waitLeaderReady: waitLeaderReady,
	}
	for _, opt := range opts {
		opt(a)
	}

	// Use noop meter when metrics are disabled to eliminate contention
	// from OTel histogram/counter internals under high concurrency.
	var meter metric.Meter
	if a.metricsEnabled {
		meter = meterProvider.Meter("admission")
	} else {
		meter = noop.Meter{}
	}

	commandDurationHistogram, err := meter.Int64Histogram(
		"admission.command.duration",
		metric.WithDescription("Total time to resolve a command (from Apply call to future resolution)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 1000, 5000, 20000, 100000, 500000, 2000000,
		),
	)
	if err != nil {
		panic(err)
	}

	commandSizeHistogram, err := meter.Int64Histogram(
		"admission.command.size",
		metric.WithDescription("Size of marshalled Raft commands in bytes"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(
			0, 512, 2048, 8192, 32768, 131072, 524288,
		),
	)
	if err != nil {
		panic(err)
	}

	proposeQueueLoadHistogram, err := meter.Int64Histogram(
		"admission.propose_queue.load",
		metric.WithDescription("Propose queue load"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	proposeQueueFullCounter, err := meter.Float64Counter(
		"admission.propose_queue.full",
		metric.WithDescription("Number of times the propose queue was full"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	proposeDurationHistogram, err := meter.Int64Histogram(
		"admission.propose.duration",
		metric.WithDescription("Time waiting for Raft to accept and replicate a proposal (Propose + Wait)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 1000, 5000, 20000, 100000, 500000, 2000000,
		),
	)
	if err != nil {
		panic(err)
	}

	fsmFutureWaitHistogram, err := meter.Int64Histogram(
		"admission.fsm_future.wait.duration",
		metric.WithDescription("Time waiting for FSM to apply the command after Raft acceptance. Spikes here indicate gating or pipeline stalls."),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 1000, 5000, 20000, 100000, 500000, 2000000,
		),
	)
	if err != nil {
		panic(err)
	}

	proposalGuardDurationHistogram, err := meter.Int64Histogram(
		"admission.proposal_guard.duration",
		metric.WithDescription("Time spent waiting to acquire the proposal guard lock"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 2000, 10000, 50000, 200000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	proposalGuardRebuildCounter, err := meter.Int64Counter(
		"admission.proposal_guard.rebuild",
		metric.WithDescription("Number of times the proposal guard had to rebuild preloads due to boundary shift"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	preloadDurationHistogram, err := meter.Int64Histogram(
		"admission.preload.duration",
		metric.WithDescription("Time spent loading preload values from store"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 2000, 10000, 50000, 200000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	preloadCounter, err := meter.Int64Counter(
		"admission.preload.total",
		metric.WithDescription("Total number of preload operations from store"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	preloadKeysNeededCounter, err := meter.Int64Counter(
		"admission.preload.keys_needed",
		metric.WithDescription("Total number of keys that needed resolving during preload"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	preloadCacheHitsCounter, err := meter.Int64Counter(
		"admission.preload.cache_hits",
		metric.WithDescription("Total number of keys found guaranteed in cache (no store read needed)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	a.commandDurationHistogram = commandDurationHistogram
	a.commandSizeHistogram = commandSizeHistogram
	a.proposeQueueLoadHistogram = proposeQueueLoadHistogram
	a.proposeQueueFullCounter = proposeQueueFullCounter
	a.proposeDurationHistogram = proposeDurationHistogram
	a.fsmFutureWaitHistogram = fsmFutureWaitHistogram
	a.proposalGuardDurationHistogram = proposalGuardDurationHistogram
	a.proposalGuardRebuildCounter = proposalGuardRebuildCounter
	a.preloadDurationHistogram = preloadDurationHistogram
	a.preloadCounter = preloadCounter
	a.preloadKeysNeededCounter = preloadKeysNeededCounter
	a.preloadCacheHitsCounter = preloadCacheHitsCounter

	return a
}

// Admit implements the ctrl.Admission interface.
// Preload Strategy for Volumes:
// 1. Volumes transition from store to cache after rotation at index R
// 2. For operations at nextIndex N, a volume V is guaranteed in cache if N > R(V)
// 3. When not guaranteed, load base value from store at boundary B(nextIndex)
// 4. For volumes not guaranteed in cache, load base values from store at B(nextIndex)
// 5. Propose command with Preload containing base values.
func (a *Admission) Admit(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
	if err := a.writeGate.CheckWritesAllowed(); err != nil {
		return nil, err
	}

	// Wait for the FSM to be caught up after a leadership transition.
	// This ensures admission pre-reads (revert postings, numscript resolution)
	// see all committed entries from the previous leader.
	if err := a.waitLeaderReady(ctx); err != nil {
		return nil, fmt.Errorf("waiting for leader readiness: %w", err)
	}

	// Verify the batch signature (if any) and unwrap the trusted ApplyBatch.
	// A signed batch is opaque: the signature is verified against the payload
	// bytes and the trusted batch (ordered requests + idempotency key) is
	// unmarshaled from those bytes — signing the batch authenticates its
	// composition and ordering, not just individual request content.
	ctx, sigSpan := tracer.Start(ctx, "admission.verify_signatures")
	batch, err := a.resolveBatch(req)

	sigSpan.End()

	if err != nil {
		return nil, err
	}

	// Check maintenance mode: block all requests except SetMaintenanceMode.
	if a.sharedState.MaintenanceMode() && !allRequestsAreMaintenanceMode(batch.requests) {
		return nil, ErrMaintenanceMode
	}

	// Convert requests to orders. Idempotency and signature are batch-level now
	// (carried on the Proposal), so orders no longer hold either.
	orders, overlay, err := a.requestsToOrders(ctx, batch.requests, batch.sig)
	if err != nil {
		return nil, fmt.Errorf("converting requests to orders: %w", err)
	}

	// Step 1: Extract preload needs from orders (excludes script-dependent needs)
	needs, perOrder, err := a.extractPreloadNeeds(ctx, orders)
	if err != nil {
		return nil, err
	}

	// The batch idempotency key is preloaded once for the whole proposal. It
	// rides on the first order's needs — idempotency keys are not coverage-gated
	// (machine.Preload installs them unconditionally), so any order carries it
	// and the FSM's per-proposal dedup check finds it. Empty key = no idempotency.
	if batch.key != "" && len(orders) > 0 {
		needs.IdempotencyKeys[domain.IdempotencyKey{Key: batch.key}] = struct{}{}
		perOrder[0].IdempotencyKeys[domain.IdempotencyKey{Key: batch.key}] = struct{}{}
	}

	// Step 2: Resolve script references and discover script dependencies.
	// This enriches needs with volumes/metadata discovered from scripts.
	if err := a.resolveScriptsAndEnrichNeeds(ctx, orders, overlay, needs, perOrder); err != nil {
		return nil, err
	}

	// Step 3-5: Build preloads via shared Builder (no lock)
	cmd := commands.NewCommand(orders...)
	if batch.key != "" {
		cmd.Idempotency = &commonpb.Idempotency{Key: batch.key}
	}
	cmd.Signature = batch.sig

	ctx, preloadSpan := tracer.Start(ctx, "admission.preload",
		trace.WithAttributes(
			attribute.Int("preload.ledgers", len(needs.Ledgers)),
			attribute.Int("preload.boundaries", len(needs.Boundaries)),
			attribute.Int("preload.volumes", len(needs.Volumes)),
			attribute.Int("preload.idempotency_keys", len(needs.IdempotencyKeys)),
			attribute.Int("preload.references", len(needs.References)),
			attribute.Int("preload.metadata", len(needs.Metadata)),
		))

	// Build the per-order WriteOperation slice. Each operation carries
	// its Needs (for preload aggregation) and a SetCoverage closure
	// that the runner invokes at marshal time to write the computed
	// bitset onto Order.CoverageBits.
	operations := make([]plan.WriteOperation, len(orders))
	for i := range orders {
		operations[i] = plan.WriteOperation{
			Needs: perOrder[i],
			SetCoverage: func(bits []byte) {
				cmd.GetOrders()[i].CoverageBits = bits
			},
		}
	}

	preloadStart := time.Now()
	build, err := a.builder.Build(operations)
	a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds())
	if err != nil {
		preloadSpan.End()
		build.ReleaseLoaders()

		return nil, fmt.Errorf("building preloads: %w", err)
	}

	totalKeys := int64(needs.TotalKeys())

	var storeReads int64
	for _, plan := range build.ExecutionPlan.GetAttributes() {
		if _, ok := plan.GetIntent().(*raftcmdpb.AttributePlan_Value); ok {
			storeReads++
		}
	}

	cacheHits := totalKeys - storeReads

	a.preloadCounter.Add(ctx, 1)
	a.preloadKeysNeededCounter.Add(ctx, totalKeys)
	a.preloadCacheHitsCounter.Add(ctx, cacheHits)

	cmd.ExecutionPlan = build.ExecutionPlan
	cmd.CallerSnapshot = auth.ResolveCallerSnapshot(ctx)

	preloadSpan.End()

	// Step 5: Marshal + acquire proposal guard + set PredictedIndex
	// + propose, all via the shared preload runner. The runner also
	// patches PredictedIndex onto the pre-marshaled buffer (or
	// re-marshals on the rare boundary-shift rebuild).
	//
	// Per-order coverage bits depend on the final AttributePlan slice
	// (positions in cmd.ExecutionPlan.Attributes), and AcquireProposalGuard may
	// swap cmd.ExecutionPlan for a rebuilt ExecutionPlan on a generation shift.
	// Compute the bits inside marshalFn so every (re-)marshal sees the
	// current Preload — the runner calls marshalFn again after the
	// rebuild, keeping bits and plans in sync.
	start := time.Now()

	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	ctx, proposeSpan := tracer.Start(ctx, "admission.propose")

	runResult, err := a.builder.Run(
		ctx, cmd, build,
		func(c *raftcmdpb.Proposal) ([]byte, error) {
			// Coverage and productions are already assigned by the
			// runner before this callback runs. We just marshal + emit
			// admission metrics.
			return a.marshalCommand(ctx, c)
		},
		a.proposer,
	)
	if err != nil {
		proposeSpan.End()

		// Distinguish proposer error (queue full / shutdown) from
		// marshal / guard errors via the runner's phase sentinels.
		// The marshal and guard wrappers carry their own diagnostic
		// already; the bare propose error is the queue-full case.
		if !errors.Is(err, plan.ErrMarshalProposal) && !errors.Is(err, plan.ErrAcquireProposalGuard) {
			a.logger.WithFields(map[string]any{
				"channel": "raft.node.propose",
			}).Errorf("Proposal failed: %v", err)
			a.proposeQueueFullCounter.Add(context.Background(), 1)
		}

		return nil, err
	}

	if runResult.Rebuilt {
		a.proposalGuardRebuildCounter.Add(ctx, 1)
	}

	a.proposalGuardDurationHistogram.Record(ctx, runResult.LockHeldDuration.Microseconds())
	a.proposeQueueLoadHistogram.Record(context.Background(), int64(a.proposeQueueInflight.Add(1)))

	guard := runResult.Guard
	fsmFuture := runResult.FSMFuture
	proposal := runResult.Proposal

	if _, err := proposal.Wait(ctx); err != nil {
		proposeSpan.End()
		guard.ReleaseLoaders()
		a.proposeQueueInflight.Add(-1)

		return nil, err
	}

	// Old semantic preserved: "admission.propose.duration" measures
	// Propose + Wait combined (queue insertion through Raft commit
	// acceptance). The runner exposes ProposeStartTime so we can
	// compute this without holding our own timer.
	a.proposeDurationHistogram.Record(ctx, time.Since(runResult.ProposeStartTime).Microseconds())
	proposeSpan.End()

	// Ensure cleanup runs on all paths after proposal acceptance (success and error).
	defer a.proposeQueueInflight.Add(-1)
	defer guard.ReleaseLoaders()

	// Wait for FSM to apply the command
	ctx, fsmSpan := tracer.Start(ctx, "admission.fsm_wait")
	defer fsmSpan.End()

	fsmWaitStart := time.Now()
	result, err := fsmFuture.Wait(ctx)
	if err != nil {
		return nil, err
	}

	a.fsmFutureWaitHistogram.Record(ctx, time.Since(fsmWaitStart).Microseconds())

	// Resolve CreatedLogOrReference entries into concrete logs.
	// Created logs are returned directly; reference sequences (idempotent responses)
	// are fetched from PebbleDB here on the parallel path, outside the FSM hot path.
	logs := make([]*commonpb.Log, len(result.Logs))
	for i, logOrRef := range result.Logs {
		if created := logOrRef.GetCreatedLog(); created != nil {
			logs[i] = created
		} else if refSeq := logOrRef.GetReferenceSequence(); refSeq > 0 {
			log, fetchErr := query.ReadLogBySequence(ctx, a.store, refSeq)
			if fetchErr != nil {
				return nil, fmt.Errorf("fetching referenced log %d for idempotent response: %w", refSeq, fetchErr)
			}

			logs[i] = log
		}
	}

	return logs, err
}

// Barrier proposes a no-op command through Raft and waits for it to be applied.
// When Barrier returns, all previously proposed entries are guaranteed applied
// due to Raft log ordering. Returns the Raft commit index at which the barrier was applied.
func (a *Admission) Barrier(ctx context.Context) (uint64, error) {
	// Barrier proposes a no-op entry that still appends to the Raft WAL and
	// consumes consensus capacity like a write, so it must honor the same
	// write gate as Admit. Without this, a disk-full leader would keep growing
	// the WAL via Barrier even while normal writes return WRITES_BLOCKED_DISK_FULL.
	if err := a.writeGate.CheckWritesAllowed(); err != nil {
		return 0, err
	}

	cmd := commands.NewCommand() // no orders = no-op
	proposalData, err := a.marshalCommand(ctx, cmd)
	if err != nil {
		return 0, fmt.Errorf("marshaling barrier command: %w", err)
	}

	proposal := node.NewProposal(cmd.GetId(), proposalData)

	// Lock the tracker to serialize the Increment with guarded proposals,
	// preventing preload boundary mismatches in the FSM.
	a.builder.LockTracker()
	fsmFuture, err := a.proposer.Propose(ctx, proposal)
	a.builder.UnlockTracker()

	if err != nil {
		return 0, err
	}

	if _, err := proposal.Wait(ctx); err != nil {
		return 0, err
	}

	result, err := fsmFuture.Wait(ctx)
	if err != nil {
		return 0, err
	}

	// A successfully resolved barrier future must carry the Raft index of its
	// own no-op entry; zero means the futures bookkeeping resolved the wrong
	// entry (applier ownership transfer) and the caller would treat an
	// unpositioned barrier as a valid quiescence point.
	if result.AppliedIndex == 0 {
		assert.Unreachable("barrier future resolved with zero applied index", map[string]any{
			"commandId": cmd.GetId(),
		})
	}

	return result.AppliedIndex, nil
}

// marshalCommand marshals a proposal command into a newly allocated byte slice
// using a pooled buffer. The returned slice is safe for Raft retention.
func (a *Admission) marshalCommand(ctx context.Context, cmd *raftcmdpb.Proposal) ([]byte, error) {
	_, marshalSpan := tracer.Start(ctx, "admission.marshal")
	defer marshalSpan.End()

	proposalData, err := vtmarshal.MarshalCopy(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	a.commandSizeHistogram.Record(ctx, int64(len(proposalData)))
	marshalSpan.SetAttributes(attribute.Int("command.size_bytes", len(proposalData)))

	return proposalData, nil
}

// verifiedBatch is the trusted content of an ApplyRequest after signature
// verification: the ordered requests, the batch idempotency key, and the
// signing envelope (nil if unsigned), propagated onto the Proposal for audit.
type verifiedBatch struct {
	requests []*servicepb.Request
	key      string
	sig      *signaturepb.SignedApplyBatch
}

// resolveBatch verifies the batch signature (if any), unwraps the trusted
// ApplyBatch, and validates the idempotency key. A signed batch is opaque: the
// signature is verified against the payload bytes and the trusted batch is
// unmarshaled from them — the server never re-serializes. An unsigned batch is
// admitted only when signatures are not required (or for signing bootstrap).
func (a *Admission) resolveBatch(req *servicepb.ApplyRequest) (verifiedBatch, error) {
	switch v := req.GetVariant().(type) {
	case *servicepb.ApplyRequest_Signed:
		sb := v.Signed

		pubKey := a.keyStore.GetPublicKey(sb.GetKeyId())
		if pubKey == nil {
			return verifiedBatch{}, fmt.Errorf("%w: %s", signing.ErrUnknownKeyID, sb.GetKeyId())
		}

		if err := signing.Verify(sb, pubKey); err != nil {
			return verifiedBatch{}, err
		}

		batch, err := signing.ExtractBatch(sb)
		if err != nil {
			return verifiedBatch{}, fmt.Errorf("extracting signed batch: %w", err)
		}

		if err := validateIdempotencyKey(batch.GetIdempotencyKey()); err != nil {
			return verifiedBatch{}, err
		}

		return verifiedBatch{requests: batch.GetRequests(), key: batch.GetIdempotencyKey(), sig: sb}, nil
	case *servicepb.ApplyRequest_Unsigned:
		batch := v.Unsigned
		if batch == nil {
			return verifiedBatch{}, fmt.Errorf("%w: empty unsigned batch", signing.ErrMissingSignature)
		}

		if err := a.authorizeUnsignedBatch(batch.GetRequests()); err != nil {
			return verifiedBatch{}, err
		}

		if err := validateIdempotencyKey(batch.GetIdempotencyKey()); err != nil {
			return verifiedBatch{}, err
		}

		return verifiedBatch{requests: batch.GetRequests(), key: batch.GetIdempotencyKey()}, nil
	default:
		return verifiedBatch{}, fmt.Errorf("%w: apply request has no variant", signing.ErrMissingSignature)
	}
}

// authorizeUnsignedBatch enforces the unsigned-request policy across the whole
// batch: every request must be admissible without a signature. Bootstrap logic:
//   - RegisterSigningKey is allowed unsigned when no keys exist yet (bootstrap)
//   - all other signing-management requests require a signature once keys exist
//   - regular requests check the requireSignatures flag
func (a *Admission) authorizeUnsignedBatch(reqs []*servicepb.Request) error {
	for _, req := range reqs {
		if isSigningManagementRequest(req) {
			if isRegisterSigningKeyRequest(req) && !a.keyStore.HasKeys() {
				continue
			}

			return signing.ErrMissingSignature
		}

		if a.sharedState.RequireSignatures() {
			return signing.ErrMissingSignature
		}
	}

	return nil
}

// validateIdempotencyKey enforces the length and UTF-8 bounds on a batch key.
func validateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}

	if len(key) > maxIdempotencyKeyLength {
		return &domain.BusinessError{Err: ErrIdempotencyKeyTooLong}
	}

	if !utf8.ValidString(key) {
		return &domain.BusinessError{Err: ErrIdempotencyKeyInvalidUTF8}
	}

	return nil
}

// isSigningManagementRequest returns true if the request is a signing key
// management operation (register, revoke, or config change).
func isSigningManagementRequest(req *servicepb.Request) bool {
	switch req.GetType().(type) {
	case *servicepb.Request_RegisterSigningKey,
		*servicepb.Request_RevokeSigningKey,
		*servicepb.Request_SetSigningConfig:
		return true
	}

	return false
}

// isRegisterSigningKeyRequest returns true if the request is specifically
// a RegisterSigningKey request.
func isRegisterSigningKeyRequest(req *servicepb.Request) bool {
	_, ok := req.GetType().(*servicepb.Request_RegisterSigningKey)

	return ok
}

const maxIdempotencyKeyLength = 256

// ErrIdempotencyKeyTooLong is returned when an idempotency key exceeds the maximum length.
type errIdempotencyKeyTooLong struct{}

func (errIdempotencyKeyTooLong) Error() string {
	return "idempotency key exceeds maximum length of 256 characters"
}
func (errIdempotencyKeyTooLong) Reason() string              { return domain.ErrReasonValidation }
func (errIdempotencyKeyTooLong) Metadata() map[string]string { return nil }

var ErrIdempotencyKeyTooLong domain.Describable = errIdempotencyKeyTooLong{}

// ErrIdempotencyKeyInvalidUTF8 is returned when an idempotency key contains invalid UTF-8.
type errIdempotencyKeyInvalidUTF8 struct{}

func (errIdempotencyKeyInvalidUTF8) Error() string               { return "idempotency key contains invalid UTF-8" }
func (errIdempotencyKeyInvalidUTF8) Reason() string              { return domain.ErrReasonValidation }
func (errIdempotencyKeyInvalidUTF8) Metadata() map[string]string { return nil }

var ErrIdempotencyKeyInvalidUTF8 domain.Describable = errIdempotencyKeyInvalidUTF8{}

// ErrMaintenanceMode is returned when maintenance mode is active and the request is not a maintenance mode toggle.
// Distinct from domain.ErrMaintenanceMode (FSM-level): this one is admission-level (caller hit the gate before the
// proposal entered the Raft pipeline) and shares the same Kind/Reason wire contract.
var ErrMaintenanceMode = domain.ErrMaintenanceMode

// ErrCheckpointOrderNotLast is returned when a bulk request mixes a checkpoint
// trigger (CreateQueryCheckpoint or CloseChapter) with any non-trigger order
// AND the trigger does not occupy the last slot. The FSM commits the batch as
// a single atomic unit, so a trigger order must always be last — otherwise it
// would force a mid-batch commit that races the pipelined committer.
type errCheckpointOrderNotLast struct{}

func (errCheckpointOrderNotLast) Error() string {
	return "checkpoint trigger (CreateQueryCheckpoint or CloseChapter) must be the last order in a bulk request"
}
func (errCheckpointOrderNotLast) Reason() string              { return domain.ErrReasonValidation }
func (errCheckpointOrderNotLast) Metadata() map[string]string { return nil }

var ErrCheckpointOrderNotLast domain.Describable = errCheckpointOrderNotLast{}

// allRequestsAreMaintenanceMode returns true if every request in the batch is a SetMaintenanceMode request.
func allRequestsAreMaintenanceMode(reqs []*servicepb.Request) bool {
	for _, req := range reqs {
		if _, ok := req.GetType().(*servicepb.Request_SetMaintenanceMode); !ok {
			return false
		}
	}

	return true
}

// wrapLedgerScoped sets order.Type to a LedgerScopedOrder wrapper carrying the
// given ledger name and payload variant. The helper exists to keep the request
// dispatch readable — the unexported payload interface forces callers to build
// the wrapper struct in-place.
func wrapLedgerScoped(order *raftcmdpb.Order, ls *raftcmdpb.LedgerScopedOrder) {
	order.Type = &raftcmdpb.Order_LedgerScoped{LedgerScoped: ls}
}

// wrapSystemScoped sets order.Type to a SystemScopedOrder wrapper carrying
// the given payload variant.
func wrapSystemScoped(order *raftcmdpb.Order, ss *raftcmdpb.SystemScopedOrder) {
	order.Type = &raftcmdpb.Order_SystemScoped{SystemScoped: ss}
}

// addVolumeNeed adds a volume key to the preload needs.
func addVolumeNeed(p *plan.Needs, ledgerName string, account, asset string) {
	p.Volumes[domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Asset:      asset,
	}] = struct{}{}
}

// addTransactionTargetNeeds preloads the TransactionState entry for a
// TargetTransaction so the FSM can read it from cache.
func addTransactionTargetNeeds(p *plan.Needs, ledgerName string, txID uint64) {
	p.Transactions[domain.TransactionKey{
		LedgerName: ledgerName,
		ID:         txID,
	}] = struct{}{}
}

// extractLedgerScopedNeeds populates the preload Needs for a ledger-scoped
// order. The ledger lives once on the wrapper; every payload variant reads it
// from there instead of carrying its own field.
func extractLedgerScopedNeeds(p *plan.Needs, ls *raftcmdpb.LedgerScopedOrder) {
	ledgerName := ls.GetLedger()
	ledgerKey := domain.LedgerKey{Name: ledgerName}

	switch payload := ls.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_CreateLedger:
		p.Ledgers[ledgerKey] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_DeleteLedger:
		p.Ledgers[ledgerKey] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_PromoteLedger:
		p.Ledgers[ledgerKey] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_MirrorIngest:
		p.Ledgers[ledgerKey] = struct{}{}
		p.Boundaries[ledgerKey] = struct{}{}

		mi := payload.MirrorIngest

		var postings []*commonpb.Posting
		if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
			postings = ct.GetPostings()
		} else if rt := mi.GetEntry().GetRevertedTransaction(); rt != nil {
			postings = rt.GetReversePostings()
		}

		for _, posting := range postings {
			addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset())
			addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset())
		}

		if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
			for account, mm := range ct.GetAccountMetadata() {
				for key := range mm.GetValues() {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
						Key:        key,
					}] = struct{}{}
				}
			}
		}

		if sm := mi.GetEntry().GetSavedMetadata(); sm != nil {
			switch target := sm.GetTarget().GetTarget().(type) {
			case *commonpb.Target_Account:
				for key := range sm.GetMetadata() {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
						Key:        key,
					}] = struct{}{}
				}
			case *commonpb.Target_TransactionId:
				p.Transactions[domain.TransactionKey{
					LedgerName: ledgerName,
					ID:         target.TransactionId,
				}] = struct{}{}
			}
		}

		if dm := mi.GetEntry().GetDeletedMetadata(); dm != nil {
			switch target := dm.GetTarget().GetTarget().(type) {
			case *commonpb.Target_Account:
				p.Metadata[domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
					Key:        dm.GetKey(),
				}] = struct{}{}
			case *commonpb.Target_TransactionId:
				p.Transactions[domain.TransactionKey{
					LedgerName: ledgerName,
					ID:         target.TransactionId,
				}] = struct{}{}
			}
		}

		if rt := mi.GetEntry().GetRevertedTransaction(); rt != nil {
			p.Transactions[domain.TransactionKey{
				LedgerName: ledgerName,
				ID:         rt.GetRevertedTransactionId(),
			}] = struct{}{}
		}
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		p.Ledgers[ledgerKey] = struct{}{}
		p.PreparedQueries[domain.PreparedQueryKey{LedgerName: ledgerName, Name: payload.CreatePreparedQuery.GetQuery().GetName()}] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		p.Ledgers[ledgerKey] = struct{}{}
		p.PreparedQueries[domain.PreparedQueryKey{LedgerName: ledgerName, Name: payload.UpdatePreparedQuery.GetName()}] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		p.Ledgers[ledgerKey] = struct{}{}
		p.PreparedQueries[domain.PreparedQueryKey{LedgerName: ledgerName, Name: payload.DeletePreparedQuery.GetName()}] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_SaveNumscript:
		p.Ledgers[ledgerKey] = struct{}{}
		p.NumscriptVersions[domain.NumscriptVersionKey{LedgerName: ledgerName, Name: payload.SaveNumscript.GetName()}] = struct{}{}

		// For semver saves, preload the specific version content for immutability check.
		version := payload.SaveNumscript.GetVersion()
		if version != "" && version != "latest" {
			p.NumscriptContents[domain.NumscriptEntryKey{LedgerName: ledgerName, Name: payload.SaveNumscript.GetName(), Version: version}] = struct{}{}
		}
	case *raftcmdpb.LedgerScopedOrder_DeleteNumscript:
		p.Ledgers[ledgerKey] = struct{}{}
		p.NumscriptVersions[domain.NumscriptVersionKey{LedgerName: ledgerName, Name: payload.DeleteNumscript.GetName()}] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata:
		p.Ledgers[ledgerKey] = struct{}{}
		for key := range payload.SaveLedgerMetadata.GetMetadata() {
			p.LedgerMetadata[domain.LedgerMetadataKey{LedgerName: ledgerName, Key: key}] = struct{}{}
		}
	case *raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata:
		p.Ledgers[ledgerKey] = struct{}{}
		p.LedgerMetadata[domain.LedgerMetadataKey{LedgerName: ledgerName, Key: payload.DeleteLedgerMetadata.GetKey()}] = struct{}{}
	case *raftcmdpb.LedgerScopedOrder_Apply:
		p.Boundaries[ledgerKey] = struct{}{}
		p.Ledgers[ledgerKey] = struct{}{}

		switch applyData := payload.Apply.GetData().(type) {
		case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
			if applyData.CreateTransaction.GetReference() != "" {
				p.References[domain.TransactionReferenceKey{
					LedgerName: ledgerName,
					Reference:  applyData.CreateTransaction.GetReference(),
				}] = struct{}{}
			}

			// Caller-supplied account metadata always preloads here,
			// regardless of whether the transaction is posting-based or
			// script-based. processCreateTransaction reads previous values
			// to compute previousAccountMetadata for index replay, so the
			// keys must be in the preload set even when the postings
			// themselves are discovered later by the script pass.
			for account, mm := range applyData.CreateTransaction.GetAccountMetadata() {
				for key := range mm.GetValues() {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
						Key:        key,
					}] = struct{}{}
				}
			}

			// Volumes for script-based orders are discovered in a separate
			// pass (resolveScriptsAndEnrichNeeds) after extractPreloadNeeds
			// returns. Skip the posting-driven volume preload for those.
			scriptBacked := applyData.CreateTransaction.GetNumscriptReference() != nil ||
				(applyData.CreateTransaction.GetScript() != nil &&
					applyData.CreateTransaction.GetScript().GetPlain() != "" &&
					len(applyData.CreateTransaction.GetPostings()) == 0)

			if !scriptBacked {
				for _, posting := range applyData.CreateTransaction.GetPostings() {
					addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset())
					addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset())
				}
			}

		case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
			p.Transactions[domain.TransactionKey{
				LedgerName: ledgerName,
				ID:         applyData.RevertTransaction.GetTransactionId(),
			}] = struct{}{}

			for _, posting := range applyData.RevertTransaction.GetOriginalPostings() {
				addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset())
				addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset())
			}

		case *raftcmdpb.LedgerApplyOrder_AddMetadata:
			if target, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
				for key := range applyData.AddMetadata.GetMetadata() {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
						Key:        key,
					}] = struct{}{}
				}
			}

			if tx, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_TransactionId); ok {
				addTransactionTargetNeeds(p, ledgerName, tx.TransactionId)
			}

		case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
			if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
				p.Metadata[domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
					Key:        applyData.DeleteMetadata.GetKey(),
				}] = struct{}{}
			}

			if tx, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_TransactionId); ok {
				addTransactionTargetNeeds(p, ledgerName, tx.TransactionId)
			}

		case *raftcmdpb.LedgerApplyOrder_CreateIndex:
			// processCreateIndex consults the registry to short-circuit on
			// READY duplicates — preload the matching entry.
			p.Indexes[domain.IndexKey{LedgerName: ledgerName, Canonical: indexes.Canonical(applyData.CreateIndex.GetId())}] = struct{}{}

		case *raftcmdpb.LedgerApplyOrder_DropIndex:
			// processDropIndex calls DeleteIndex unconditionally, but
			// preloading keeps the FSM read side consistent with invariant 3.
			p.Indexes[domain.IndexKey{LedgerName: ledgerName, Canonical: indexes.Canonical(applyData.DropIndex.GetId())}] = struct{}{}

		case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
			// Schema changes touch the matching metadata index entry to
			// flip it back to BUILDING; preload so processSetMetadataFieldType
			// finds the current state.
			p.Indexes[domain.IndexKey{
				LedgerName: ledgerName,
				Canonical:  indexes.Canonical(indexes.MetadataID(applyData.SetMetadataFieldType.GetTargetType(), applyData.SetMetadataFieldType.GetKey())),
			}] = struct{}{}

		case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
			// Removing a schema field cascades into dropping the index;
			// processRemoveMetadataFieldType probes the registry first.
			p.Indexes[domain.IndexKey{
				LedgerName: ledgerName,
				Canonical:  indexes.Canonical(indexes.MetadataID(applyData.RemoveMetadataFieldType.GetTargetType(), applyData.RemoveMetadataFieldType.GetKey())),
			}] = struct{}{}
		}
	default:
		// Loud failure for an unmapped ledger-scoped payload. The processor
		// dispatch's matching default catches the case where no handler
		// exists; this branch catches the more dangerous asymmetry: a new
		// payload variant *with* a handler whose preload-needs case is
		// missing. Per invariant 6 the FSM apply path would then read a key
		// that was never seeded, which under invariant 7 must surface
		// loudly rather than degrade to a silent cache miss (a no-op that
		// desyncs nodes).
		assert.Unreachable("admission: unmapped LedgerScopedOrder payload in extractLedgerScopedNeeds — add a needs case", map[string]any{
			"payload_type": fmt.Sprintf("%T", ls.GetPayload()),
			"ledger":       ledgerName,
		})
	}
}

// extractSystemScopedNeeds populates the preload Needs for a system-scoped
// order. Only sink-config orders contribute preload keys today; every other
// variant is enumerated as an explicit no-op so adding a new payload
// without a matching case here trips the loud default — matching the
// invariant-7 contract that an unmapped wrapper variant must fail loudly
// rather than degrade to a silent cache miss at apply time.
func extractSystemScopedNeeds(p *plan.Needs, ss *raftcmdpb.SystemScopedOrder) {
	switch payload := ss.GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_AddEventsSink:
		p.SinkConfigs[domain.SinkConfigKey{Name: payload.AddEventsSink.GetConfig().GetName()}] = struct{}{}
	case *raftcmdpb.SystemScopedOrder_RemoveEventsSink:
		p.SinkConfigs[domain.SinkConfigKey{Name: payload.RemoveEventsSink.GetName()}] = struct{}{}

	// Explicit no-op cases: every other system-scoped variant intentionally
	// touches no cache attribute. Listed individually (not lumped into
	// default) so the compiler/test layer flags new variants — see default
	// below.
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey,
		*raftcmdpb.SystemScopedOrder_RevokeSigningKey,
		*raftcmdpb.SystemScopedOrder_SetSigningConfig,
		*raftcmdpb.SystemScopedOrder_SetMaintenanceMode,
		*raftcmdpb.SystemScopedOrder_CloseChapter,
		*raftcmdpb.SystemScopedOrder_SealChapter,
		*raftcmdpb.SystemScopedOrder_ArchiveChapter,
		*raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter,
		*raftcmdpb.SystemScopedOrder_SetChapterSchedule,
		*raftcmdpb.SystemScopedOrder_DeleteChapterSchedule,
		*raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint,
		*raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint,
		*raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule,
		*raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule:
		// Nothing to preload; payload identifier silenced via underscore
		// — the case exists purely so the default catches genuinely new
		// variants.
		_ = payload

	default:
		assert.Unreachable("admission: unmapped SystemScopedOrder payload in extractSystemScopedNeeds — add an explicit case (preload or no-op)", map[string]any{
			"payload_type": fmt.Sprintf("%T", ss.GetPayload()),
		})
	}
}

// extractPreloadNeeds extracts all preload keys from orders in a single pass.
// Returns the proposal-wide aggregate Needs and a parallel slice with one
// Needs per order (used to compute Order.coverage_bits after Build).
func (a *Admission) extractPreloadNeeds(ctx context.Context, orders []*raftcmdpb.Order) (*plan.Needs, []*plan.Needs, error) {
	aggregate := plan.NewNeeds()
	perOrder := make([]*plan.Needs, len(orders))

	for orderIdx, order := range orders {
		p := plan.NewNeeds()

		switch orderType := order.GetType().(type) {
		case *raftcmdpb.Order_LedgerScoped:
			extractLedgerScopedNeeds(p, orderType.LedgerScoped)
		case *raftcmdpb.Order_SystemScoped:
			extractSystemScopedNeeds(p, orderType.SystemScoped)
		}

		perOrder[orderIdx] = p
		aggregate.Merge(p)
	}

	return aggregate, perOrder, nil
}

// resolveScriptsAndEnrichNeeds resolves ScriptReferences and discovers volume/metadata
// dependencies from all script-based CreateTransaction orders. It enriches the given
// Needs with the discovered dependencies so that a single Build call covers everything.
//
// This runs after extractPreloadNeeds (which preloads caller-supplied accountMetadata
// keys but skips posting-driven volumes for script-based orders) and before Build.
func (a *Admission) resolveScriptsAndEnrichNeeds(ctx context.Context, orders []*raftcmdpb.Order, overlay *bulkOverlay, p *plan.Needs, perOrder []*plan.Needs) error {
	for orderIdx, order := range orders {
		ls := order.GetLedgerScoped()
		if ls == nil {
			continue
		}

		applyPayload, ok := ls.GetPayload().(*raftcmdpb.LedgerScopedOrder_Apply)
		if !ok {
			continue
		}

		createTx, ok := applyPayload.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction)
		if !ok {
			continue
		}

		// Script-discovered keys belong to this order's coverage. perOrder
		// is initialized by extractPreloadNeeds with one entry per input
		// order, so the index lookup is safe.
		orderNeeds := perOrder[orderIdx]

		ledgerName := ls.GetLedger()

		var scriptText string
		var scriptVars map[string]string
		isReference := false

		// Resolve ScriptReference: load numscript content from overlay (intra-bulk) or Pebble.
		var resolvedVersion string

		if ref := createTx.CreateTransaction.GetNumscriptReference(); ref != nil && ref.GetName() != "" {
			content, rv, err := a.resolveNumscriptReference(overlay, ledgerName, ref.GetName(), ref.GetVersion())
			if err != nil {
				return err
			}

			scriptText = content
			scriptVars = ref.GetVars()
			resolvedVersion = rv
			isReference = true

			// Replace the entire NumscriptReference rather than mutating a field
			// on the committed order's shared pointer.
			createTx.CreateTransaction.NumscriptReference = &raftcmdpb.NumscriptReference{
				Name:    ref.GetName(),
				Version: resolvedVersion,
				Vars:    ref.GetVars(),
			}
		} else if createTx.CreateTransaction.GetScript() != nil &&
			createTx.CreateTransaction.GetScript().GetPlain() != "" &&
			len(createTx.CreateTransaction.GetPostings()) == 0 {
			// Inline script
			script := createTx.CreateTransaction.GetScript()
			scriptText = script.GetPlain()
			scriptVars = script.GetVars()
		} else {
			// Postings-only — handled by extractPreloadNeeds
			continue
		}

		discovered, err := numscript.DiscoverNumscriptDependencies(
			a.numscriptCache,
			scriptText,
			scriptVars,
			ledgerName,
		)
		if err != nil {
			return &domain.BusinessError{Err: &domain.ErrDependencyDiscoveryFailed{Cause: err}}
		}

		if discovered != nil {
			for key := range discovered.SourceVolumes {
				addVolumeNeed(p, key.LedgerName, key.Account, key.Asset)
				addVolumeNeed(orderNeeds, key.LedgerName, key.Account, key.Asset)
			}

			for key := range discovered.DestinationVolumes {
				addVolumeNeed(p, key.LedgerName, key.Account, key.Asset)
				addVolumeNeed(orderNeeds, key.LedgerName, key.Account, key.Asset)
			}

			for key := range discovered.WrittenMetadata {
				p.Metadata[key] = struct{}{}
				orderNeeds.Metadata[key] = struct{}{}
			}
		}

		// For references: preload the resolved content keyed by (ledger, name, version).
		// The FSM resolves via NumscriptReference from the dual-gen cache.
		// For inline scripts: the text stays in the order as-is, no preload needed.
		if isReference {
			ref := createTx.CreateTransaction.GetNumscriptReference()
			contentKey := domain.NumscriptEntryKey{
				LedgerName: ledgerName,
				Name:       ref.GetName(),
				Version:    resolvedVersion,
			}
			p.NumscriptContents[contentKey] = struct{}{}
			orderNeeds.NumscriptContents[contentKey] = struct{}{}
		}
	}

	return nil
}

// requestToOrder converts a single Request into its ledger- or system-scoped
// raftcmdpb.Order. batchSig is consulted only by the signing-key registration
// path, to record the signing key as the new key's parent.
func (a *Admission) requestToOrder(ctx context.Context, req *servicepb.Request, batchSig *signaturepb.SignedApplyBatch, overlay *bulkOverlay) (*raftcmdpb.Order, error) {
	order := &raftcmdpb.Order{}

	switch reqType := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.CreateLedger.GetName(),
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
				CreateLedger: &raftcmdpb.CreateLedgerOrder{
					InitialSchema:          reqType.CreateLedger.GetInitialSchema(),
					Mode:                   reqType.CreateLedger.GetMode(),
					MirrorSource:           reqType.CreateLedger.GetMirrorSource(),
					AccountTypes:           reqType.CreateLedger.GetAccountTypes(),
					DefaultEnforcementMode: reqType.CreateLedger.GetDefaultEnforcementMode(),
				},
			},
		})
	case *servicepb.Request_DeleteLedger:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.DeleteLedger.GetName(),
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{
				DeleteLedger: &raftcmdpb.DeleteLedgerOrder{},
			},
		})
	case *servicepb.Request_Apply:
		applyOrder, err := a.convertApplyRequest(ctx, reqType.Apply)
		if err != nil {
			return nil, err
		}

		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.Apply.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: applyOrder,
			},
		})
	case *servicepb.Request_RegisterSigningKey:
		var parentKeyID string
		if batchSig != nil {
			parentKeyID = batchSig.GetKeyId()
		}

		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
				RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
					KeyId:       reqType.RegisterSigningKey.GetKeyId(),
					PublicKey:   reqType.RegisterSigningKey.GetPublicKey(),
					ParentKeyId: parentKeyID,
				},
			},
		})
	case *servicepb.Request_RevokeSigningKey:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
				RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
					KeyId:   reqType.RevokeSigningKey.GetKeyId(),
					Cascade: reqType.RevokeSigningKey.GetCascade(),
				},
			},
		})
	case *servicepb.Request_SetSigningConfig:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SetSigningConfig{
				SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{
					RequireSignatures: reqType.SetSigningConfig.GetRequireSignatures(),
				},
			},
		})
	case *servicepb.Request_AddEventsSink:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
				AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
					Config: reqType.AddEventsSink.GetConfig(),
				},
			},
		})

		overlay.sinks.Put(reqType.AddEventsSink.GetConfig().GetName(), reqType.AddEventsSink.GetConfig())
	case *servicepb.Request_RemoveEventsSink:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{
				RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
					Name: reqType.RemoveEventsSink.GetName(),
				},
			},
		})

		overlay.sinks.Delete(reqType.RemoveEventsSink.GetName())
	case *servicepb.Request_CloseChapter:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{
				CloseChapter: &raftcmdpb.CloseChapterOrder{},
			},
		})
	case *servicepb.Request_SealChapter:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SealChapter{
				SealChapter: &raftcmdpb.SealChapterOrder{
					ChapterId:   reqType.SealChapter.GetChapterId(),
					SealingHash: reqType.SealChapter.GetSealingHash(),
					StateHash:   reqType.SealChapter.GetStateHash(),
				},
			},
		})
	case *servicepb.Request_ArchiveChapter:
		if !a.coldStorageEnabled {
			return nil, domain.ErrColdStorageDisabled
		}

		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_ArchiveChapter{
				ArchiveChapter: &raftcmdpb.ArchiveChapterOrder{
					ChapterId: reqType.ArchiveChapter.GetChapterId(),
				},
			},
		})
	case *servicepb.Request_ConfirmArchiveChapter:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_ConfirmArchiveChapter{
				ConfirmArchiveChapter: &raftcmdpb.ConfirmArchiveChapterOrder{
					ChapterId: reqType.ConfirmArchiveChapter.GetChapterId(),
				},
			},
		})
	case *servicepb.Request_SetMaintenanceMode:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
				SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{
					Enabled: reqType.SetMaintenanceMode.GetEnabled(),
				},
			},
		})
	case *servicepb.Request_SetChapterSchedule:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{
				SetChapterSchedule: &raftcmdpb.SetChapterScheduleOrder{
					Cron: reqType.SetChapterSchedule.GetCron(),
				},
			},
		})
	case *servicepb.Request_DeleteChapterSchedule:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_DeleteChapterSchedule{
				DeleteChapterSchedule: &raftcmdpb.DeleteChapterScheduleOrder{},
			},
		})
	case *servicepb.Request_PromoteLedger:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.PromoteLedger.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
				PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
			},
		})
	case *servicepb.Request_CreatePreparedQuery:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.CreatePreparedQuery.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
				CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
					Query: reqType.CreatePreparedQuery.GetQuery(),
				},
			},
		})
	case *servicepb.Request_UpdatePreparedQuery:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.UpdatePreparedQuery.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{
				UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{
					Name:   reqType.UpdatePreparedQuery.GetName(),
					Filter: reqType.UpdatePreparedQuery.GetFilter(),
				},
			},
		})
	case *servicepb.Request_DeletePreparedQuery:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.DeletePreparedQuery.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{
				DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{
					Name: reqType.DeletePreparedQuery.GetName(),
				},
			},
		})
	case *servicepb.Request_SetMetadataFieldType:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.SetMetadataFieldType.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: reqType.SetMetadataFieldType.GetTargetType(),
							Key:        reqType.SetMetadataFieldType.GetKey(),
							Type:       reqType.SetMetadataFieldType.GetType(),
						},
					},
				},
			},
		})
	case *servicepb.Request_RemoveMetadataFieldType:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.RemoveMetadataFieldType.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
						RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
							TargetType: reqType.RemoveMetadataFieldType.GetTargetType(),
							Key:        reqType.RemoveMetadataFieldType.GetKey(),
						},
					},
				},
			},
		})
	case *servicepb.Request_CreateIndex:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.CreateIndex.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{
						Id: reqType.CreateIndex.GetId(),
					}},
				},
			},
		})
	case *servicepb.Request_DropIndex:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.DropIndex.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_DropIndex{DropIndex: &raftcmdpb.DropIndexOrder{
						Id: reqType.DropIndex.GetId(),
					}},
				},
			},
		})
	case *servicepb.Request_SaveNumscript:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.SaveNumscript.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
				SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
					Name:    reqType.SaveNumscript.GetName(),
					Content: reqType.SaveNumscript.GetContent(),
					Version: reqType.SaveNumscript.GetVersion(),
				},
			},
		})

		overlay.recordNumscriptSave(
			reqType.SaveNumscript.GetLedger(),
			reqType.SaveNumscript.GetName(),
			reqType.SaveNumscript.GetVersion(),
			reqType.SaveNumscript.GetContent(),
		)
	case *servicepb.Request_DeleteNumscript:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.DeleteNumscript.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteNumscript{
				DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{
					Name: reqType.DeleteNumscript.GetName(),
				},
			},
		})

		overlay.recordNumscriptDelete(reqType.DeleteNumscript.GetLedger(), reqType.DeleteNumscript.GetName())
	case *servicepb.Request_CreateQueryCheckpoint:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
			},
		})
	case *servicepb.Request_DeleteQueryCheckpoint:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint{
				DeleteQueryCheckpoint: &raftcmdpb.DeleteQueryCheckpointOrder{
					CheckpointId: reqType.DeleteQueryCheckpoint.GetCheckpointId(),
				},
			},
		})
	case *servicepb.Request_SetQueryCheckpointSchedule:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{
					Cron: reqType.SetQueryCheckpointSchedule.GetCron(),
				},
			},
		})
	case *servicepb.Request_DeleteQueryCheckpointSchedule:
		wrapSystemScoped(order, &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &raftcmdpb.DeleteQueryCheckpointScheduleOrder{},
			},
		})
	case *servicepb.Request_AddAccountType:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.AddAccountType.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
						AddAccountType: &raftcmdpb.AddAccountTypeOrder{
							AccountType: reqType.AddAccountType.GetAccountType(),
						},
					},
				},
			},
		})
	case *servicepb.Request_RemoveAccountType:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.RemoveAccountType.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
						RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{
							Name: reqType.RemoveAccountType.GetName(),
						},
					},
				},
			},
		})
	case *servicepb.Request_SetDefaultEnforcementMode:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.SetDefaultEnforcementMode.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{
				Apply: &raftcmdpb.LedgerApplyOrder{
					Data: &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{
						UpdateDefaultEnforcementMode: &raftcmdpb.UpdateDefaultEnforcementModeOrder{
							EnforcementMode: reqType.SetDefaultEnforcementMode.GetEnforcementMode(),
						},
					},
				},
			},
		})
	case *servicepb.Request_SaveLedgerMetadata:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.SaveLedgerMetadata.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
				SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
					Metadata: reqType.SaveLedgerMetadata.GetMetadata(),
				},
			},
		})
	case *servicepb.Request_DeleteLedgerMetadata:
		wrapLedgerScoped(order, &raftcmdpb.LedgerScopedOrder{
			Ledger: reqType.DeleteLedgerMetadata.GetLedger(),
			Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{
				DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{
					Key: reqType.DeleteLedgerMetadata.GetKey(),
				},
			},
		})
	default:
		return nil, fmt.Errorf("unsupported request type: %T", req.GetType())
	}

	// Validate storage-safety invariants (null bytes in ledger names, metadata keys, etc.)
	if err := validateOrder(order); err != nil {
		return nil, err
	}

	return order, nil
}

// convertApplyRequest converts a servicepb.LedgerApplyRequest to a
// raftcmdpb.LedgerApplyOrder payload. The ledger name lives on the
// surrounding LedgerScopedOrder wrapper; callers must set it there.
func (a *Admission) convertApplyRequest(ctx context.Context, apply *servicepb.LedgerApplyRequest) (*raftcmdpb.LedgerApplyOrder, error) {
	order := &raftcmdpb.LedgerApplyOrder{}

	switch data := apply.GetAction().GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		ct := data.CreateTransaction
		script := ct.GetScript()

		var numscriptRef *raftcmdpb.NumscriptReference

		// ScriptReference validation: reject if both script content and reference are set.
		// Content resolution is deferred to extractPreloadNeeds.
		if ct.GetScriptReference() != nil {
			if script != nil && script.GetPlain() != "" {
				return nil, &domain.BusinessError{
					Err: domain.ErrScriptAndReferenceConflict,
				}
			}

			// Pass vars through; content will be resolved in resolveScriptsAndEnrichNeeds
			script = &commonpb.Script{
				Vars: ct.GetScriptReference().GetVars(),
			}

			numscriptRef = &raftcmdpb.NumscriptReference{
				Name:    ct.GetScriptReference().GetName(),
				Version: ct.GetScriptReference().GetVersion(),
				Vars:    ct.GetScriptReference().GetVars(),
			}
		}

		order.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Postings:           ct.GetPostings(),
				Script:             script,
				Timestamp:          ct.GetTimestamp(),
				Reference:          ct.GetReference(),
				Metadata:           ct.GetMetadata(),
				AccountMetadata:    ct.GetAccountMetadata(),
				Force:              ct.GetForce(),
				ExpandVolumes:      ct.GetExpandVolumes(),
				NumscriptReference: numscriptRef,
			},
		}
	case *servicepb.LedgerAction_AddMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
			AddMetadata: &raftcmdpb.SaveMetadataOrder{
				Target:   data.AddMetadata.GetTarget(),
				Metadata: data.AddMetadata.GetMetadata(),
			},
		}
	case *servicepb.LedgerAction_DeleteMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
			DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
				Target: data.DeleteMetadata.GetTarget(),
				Key:    data.DeleteMetadata.GetKey(),
			},
		}
	case *servicepb.LedgerAction_AddAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_AddAccountType{
			AddAccountType: &raftcmdpb.AddAccountTypeOrder{
				AccountType: data.AddAccountType.GetAccountType(),
			},
		}
	case *servicepb.LedgerAction_RemoveAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
			RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{
				Name: data.RemoveAccountType.GetName(),
			},
		}
	case *servicepb.LedgerAction_SetDefaultEnforcementMode:
		order.Data = &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{
			UpdateDefaultEnforcementMode: &raftcmdpb.UpdateDefaultEnforcementModeOrder{
				EnforcementMode: data.SetDefaultEnforcementMode.GetEnforcementMode(),
			},
		}
	case *servicepb.LedgerAction_RevertTransaction:
		// Resolve the target transaction id. When the caller supplied a
		// reference, look it up in the store; Revert never resolves against
		// the current batch because that would require reading writes that
		// have not been committed yet.
		txID, err := a.resolveRevertTarget(ctx, apply.GetLedger(), data.RevertTransaction)
		if err != nil {
			return nil, err
		}

		var originalPostings []*commonpb.Posting

		if data.RevertTransaction.GetReceipt() != "" && a.receiptSigner != nil {
			// Verify receipt and extract postings
			claims, err := a.receiptSigner.Verify(data.RevertTransaction.GetReceipt())
			if err != nil {
				return nil, fmt.Errorf("invalid receipt: %w", err)
			}

			if claims.Ledger != apply.GetLedger() {
				return nil, fmt.Errorf("receipt ledger %q does not match request ledger %q", claims.Ledger, apply.GetLedger())
			}

			if claims.TxID != txID {
				return nil, fmt.Errorf("receipt txID %d does not match resolved txID %d", claims.TxID, txID)
			}

			originalPostings = receipt.ClaimsToPostings(claims.Postings)
		} else {
			// Fall back to reading from Pebble
			originalPostings, err = a.getTransactionPostings(apply.GetLedger(), txID)
			if err != nil {
				return nil, fmt.Errorf("getting original transaction postings: %w", err)
			}
		}

		order.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
			RevertTransaction: &raftcmdpb.RevertTransactionOrder{
				TransactionId:    txID,
				Force:            data.RevertTransaction.GetForce(),
				AtEffectiveDate:  data.RevertTransaction.GetAtEffectiveDate(),
				Metadata:         data.RevertTransaction.GetMetadata(),
				OriginalPostings: originalPostings,
				ExpandVolumes:    data.RevertTransaction.GetExpandVolumes(),
			},
		}
	default:
		return nil, fmt.Errorf("unsupported apply data type: %T", apply.GetAction().GetData())
	}

	return order, nil
}

// requestsToOrders converts a slice of servicepb.Request to raftcmdpb.Order.
// resolveNumscriptReference resolves a numscript reference from the overlay (intra-bulk) or Pebble.
func (a *Admission) resolveNumscriptReference(overlay *bulkOverlay, ledgerName string, name, version string) (string, string, error) {
	if content, resolvedVersion, found := a.resolveNumscriptFromOverlay(overlay, ledgerName, name, version); found {
		return content, resolvedVersion, nil
	}

	if overlay.numscriptLatest.IsDeleted(numscriptNameKey{Ledger: ledgerName, Name: name}) {
		return "", "", &domain.BusinessError{Err: &domain.ErrNumscriptNotFound{Name: name}}
	}

	nsHandle, handleErr := a.store.NewDirectReadHandle()
	if handleErr != nil {
		return "", "", fmt.Errorf("creating read handle: %w", handleErr)
	}
	defer func() { _ = nsHandle.Close() }()

	info, err := query.ReadNumscript(a.attrs.NumscriptVersion, a.attrs.NumscriptContent, nsHandle, ledgerName, name, version)
	if err != nil {
		return "", "", fmt.Errorf("reading numscript %q: %w", name, err)
	}

	if info == nil {
		return "", "", &domain.BusinessError{Err: &domain.ErrNumscriptNotFound{Name: name}}
	}

	return info.GetContent(), info.GetVersion(), nil
}

// resolveNumscriptFromOverlay tries to resolve a numscript from the intra-bulk overlay.
func (a *Admission) resolveNumscriptFromOverlay(overlay *bulkOverlay, ledger, name, version string) (string, string, bool) {
	if version == "" {
		latestVer, ok := overlay.numscriptLatest.Get(numscriptNameKey{Ledger: ledger, Name: name})
		if !ok {
			return "", "", false
		}

		content, ok := overlay.numscriptEntries.Get(numscriptEntryKey{Ledger: ledger, Name: name, Version: latestVer})
		if !ok {
			return "", "", false
		}

		return content, latestVer, true
	}

	if version == "latest" {
		content, ok := overlay.numscriptEntries.Get(numscriptEntryKey{Ledger: ledger, Name: name, Version: "latest"})
		if !ok {
			return "", "", false
		}

		return content, "latest", true
	}

	// Exact semver lookup
	if content, ok := overlay.numscriptEntries.Get(numscriptEntryKey{Ledger: ledger, Name: name, Version: version}); ok {
		return content, version, true
	}

	// Partial semver: iterate overlay entries and find highest match
	major, minor, _, depth, err := semver.ParsePartial(version)
	if err != nil || depth == 3 {
		return "", "", false
	}

	var (
		bestContent string
		bestVersion semver.Version
		found       bool
	)

	overlay.numscriptEntries.Range(func(key numscriptEntryKey, content string) bool {
		if key.Ledger != ledger || key.Name != name || key.Version == "latest" {
			return true
		}

		v, parseErr := semver.Parse(key.Version)
		if parseErr != nil {
			return true
		}

		if v.Major != major {
			return true
		}

		if depth >= 2 && v.Minor != minor {
			return true
		}

		if !found || v.Major > bestVersion.Major ||
			(v.Major == bestVersion.Major && v.Minor > bestVersion.Minor) ||
			(v.Major == bestVersion.Major && v.Minor == bestVersion.Minor && v.Patch > bestVersion.Patch) {
			bestContent = content
			bestVersion = v
			found = true
		}

		return true
	})

	if !found {
		return "", "", false
	}

	return bestContent, bestVersion.String(), true
}

func (a *Admission) requestsToOrders(ctx context.Context, reqs []*servicepb.Request, batchSig *signaturepb.SignedApplyBatch) ([]*raftcmdpb.Order, *bulkOverlay, error) {
	overlay := newBulkOverlay()
	orders := make([]*raftcmdpb.Order, len(reqs))

	for i, req := range reqs {
		order, err := a.requestToOrder(ctx, req, batchSig, overlay)
		if err != nil {
			return nil, nil, fmt.Errorf("converting request %d: %w", i, err)
		}

		orders[i] = order
	}

	if state.ClassifyCheckpointOrderPosition(orders) == state.CheckpointOrderInvalid {
		return nil, nil, &domain.BusinessError{Err: ErrCheckpointOrderNotLast}
	}

	return orders, overlay, nil
}

// resolveRevertTarget returns the target transaction id of a Revert action.
func (a *Admission) resolveRevertTarget(_ context.Context, _ string, payload *servicepb.RevertTransactionPayload) (uint64, error) {
	id := payload.GetTransactionId()
	if id == 0 {
		return 0, &domain.BusinessError{Err: domain.ErrTransactionTargetMissing}
	}

	return id, nil
}

// getTransactionPostings retrieves the postings of an original transaction from the store.
// It uses FindTransactionCreationLog to locate the creation log and extract postings.
func (a *Admission) getTransactionPostings(ledgerName string, transactionID uint64) ([]*commonpb.Posting, error) {
	_, ok := a.builder.ResolveLedgerID(ledgerName)
	if !ok {
		return nil, &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: ledgerName}}
	}

	log, err := query.FindTransactionCreationLog(context.Background(), a.store, a.attrs.Transaction, ledgerName, transactionID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.BusinessError{Err: &domain.ErrTransactionNotFound{TransactionID: transactionID}}
		}

		return nil, fmt.Errorf("finding transaction creation log: %w", err)
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.GetLog() == nil {
		return nil, fmt.Errorf(
			"log at sequence %d for ledger %s txID %d does not contain an apply payload (got %T)",
			log.GetSequence(), ledgerName, transactionID, log.GetPayload().GetType(),
		)
	}

	switch payload := applyLog.Apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.GetTransaction() == nil {
			return nil, fmt.Errorf(
				"log at sequence %d for ledger %s txID %d has a CreatedTransaction payload but the transaction is nil",
				log.GetSequence(), ledgerName, transactionID,
			)
		}

		return payload.CreatedTransaction.GetTransaction().GetPostings(), nil
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.GetRevertTransaction() == nil {
			return nil, fmt.Errorf(
				"log at sequence %d for ledger %s txID %d has a RevertedTransaction payload but the revert transaction is nil",
				log.GetSequence(), ledgerName, transactionID,
			)
		}

		return payload.RevertedTransaction.GetRevertTransaction().GetPostings(), nil
	default:
		return nil, fmt.Errorf(
			"log at sequence %d for ledger %s txID %d has unexpected payload type %T (expected CreatedTransaction or RevertedTransaction)",
			log.GetSequence(), ledgerName, transactionID, applyLog.Apply.GetLog().GetData().GetPayload(),
		)
	}
}
