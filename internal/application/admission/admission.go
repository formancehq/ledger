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
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/infra/receipt"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/pkg/vtmarshal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
	healthChecker      health.Checker
	keyStore           *keystore.KeyStore
	sharedState        *state.SharedState
	receiptSigner      *receipt.Signer
	preloader          *preload.Preloader
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
	preloader *preload.Preloader,
	meterProvider metric.MeterProvider,
	healthChecker health.Checker,
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
		preloader:       preloader,
		healthChecker:   healthChecker,
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
func (a *Admission) Admit(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if !a.healthChecker.IsHealthy() {
		return nil, health.ErrUnhealthy
	}

	// Check maintenance mode: block all requests except SetMaintenanceMode
	if a.sharedState.MaintenanceMode() && !allRequestsAreMaintenanceMode(requests) {
		return nil, ErrMaintenanceMode
	}

	// Wait for the FSM to be caught up after a leadership transition.
	// This ensures admission pre-reads (revert postings, numscript resolution)
	// see all committed entries from the previous leader.
	if err := a.waitLeaderReady(ctx); err != nil {
		return nil, fmt.Errorf("waiting for leader readiness: %w", err)
	}

	// Verify signatures and resolve signed payloads
	ctx, sigSpan := tracer.Start(ctx, "admission.verify_signatures")
	requests, err := a.verifyAndResolveSignatures(requests)

	sigSpan.End()

	if err != nil {
		return nil, err
	}

	// Convert requests to orders
	orders, overlay, err := a.requestsToOrders(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("converting requests to orders: %w", err)
	}

	// Step 1: Extract preload needs from orders (excludes script-dependent needs)
	needs, nameToID, err := a.extractPreloadNeeds(ctx, orders)
	if err != nil {
		return nil, err
	}

	// Step 2: Resolve script references and discover script dependencies.
	// This enriches needs with volumes/metadata discovered from scripts.
	if err := a.resolveScriptsAndEnrichNeeds(ctx, orders, overlay, needs, nameToID); err != nil {
		return nil, err
	}

	// Step 2b: Resolve transaction references on metadata orders so the
	// FSM cache can be warmed with the corresponding TransactionState.
	// Without this enrichment, SET/DELETE METADATA by reference on a
	// committed transaction whose state has rotated out of the cache
	// would surface as ErrTransactionNotFound at apply time.
	if err := a.resolveMetadataReferencesAndEnrichNeeds(ctx, orders, needs); err != nil {
		return nil, err
	}

	// Step 3-5: Build preloads via shared Preloader (no lock)
	cmd := commands.NewCommand(orders...)

	ctx, preloadSpan := tracer.Start(ctx, "admission.preload",
		trace.WithAttributes(
			attribute.Int("preload.ledgers", len(needs.Ledgers)),
			attribute.Int("preload.boundaries", len(needs.Boundaries)),
			attribute.Int("preload.volumes", len(needs.Volumes)),
			attribute.Int("preload.idempotency_keys", len(needs.IdempotencyKeys)),
			attribute.Int("preload.references", len(needs.References)),
			attribute.Int("preload.metadata", len(needs.Metadata)),
		))

	preloadStart := time.Now()
	build, err := a.preloader.BuildPreloads(needs)
	a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds())
	if err != nil {
		preloadSpan.End()
		build.ReleaseLoaders()

		return nil, fmt.Errorf("building preloads: %w", err)
	}

	totalKeys := int64(needs.TotalKeys())
	storeReads := int64(len(build.PreloadSet.GetPreloads()))
	cacheHits := totalKeys - storeReads

	a.preloadCounter.Add(ctx, 1)
	a.preloadKeysNeededCounter.Add(ctx, totalKeys)
	a.preloadCacheHitsCounter.Add(ctx, cacheHits)

	cmd.Preload = build.PreloadSet
	cmd.CallerSnapshot = auth.ResolveCallerSnapshot(ctx)
	preloadSpan.End()

	// Step 5: Marshal + acquire proposal guard + set PredictedIndex
	// + propose, all via the shared preload runner. The runner also
	// patches PredictedIndex onto the pre-marshaled buffer (or
	// re-marshals on the rare boundary-shift rebuild).
	start := time.Now()

	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	ctx, proposeSpan := tracer.Start(ctx, "admission.propose")

	runResult, err := a.preloader.RunWithPreload(
		ctx, cmd, build, needs,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return a.marshalCommand(ctx, c) },
		a.proposer,
	)
	if err != nil {
		proposeSpan.End()

		// Distinguish proposer error (queue full / shutdown) from
		// marshal / guard errors via the runner's phase sentinels.
		// The marshal and guard wrappers carry their own diagnostic
		// already; the bare propose error is the queue-full case.
		if !errors.Is(err, preload.ErrMarshalProposal) && !errors.Is(err, preload.ErrAcquireProposalGuard) {
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

	if _, err := proposal.Wait(); err != nil {
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
	result, err := fsmFuture.Wait()
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
	cmd := commands.NewCommand() // no orders = no-op
	proposalData, err := a.marshalCommand(ctx, cmd)
	if err != nil {
		return 0, fmt.Errorf("marshaling barrier command: %w", err)
	}

	proposal := node.NewProposal(cmd.GetId(), proposalData)

	// Lock the tracker to serialize the Increment with guarded proposals,
	// preventing preload boundary mismatches in the FSM.
	a.preloader.LockTracker()
	fsmFuture, err := a.proposer.Propose(ctx, proposal)
	a.preloader.UnlockTracker()

	if err != nil {
		return 0, err
	}

	if _, err := proposal.Wait(); err != nil {
		return 0, err
	}

	result, err := fsmFuture.Wait()
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

// verifyAndResolveSignatures verifies signatures on requests and resolves signed payloads.
// For each signed request, it verifies the signature, then deserializes signed_payload
// to obtain the authoritative Request content (preserving the signature for propagation).
//
// Bootstrap logic for unsigned requests:
//   - RegisterSigningKey is allowed unsigned when no keys exist yet (bootstrap)
//   - All other signing management requests require a signature when keys exist
//   - Regular requests check the requireSignatures flag
func (a *Admission) verifyAndResolveSignatures(requests []*servicepb.Request) ([]*servicepb.Request, error) {
	result := make([]*servicepb.Request, len(requests))
	for i, req := range requests {
		if req.GetSignature() != nil {
			// Look up public key
			pubKey := a.keyStore.GetPublicKey(req.GetSignature().GetKeyId())
			if pubKey == nil {
				return nil, fmt.Errorf("%w: %s", signing.ErrUnknownKeyID, req.GetSignature().GetKeyId())
			}

			// Verify the signature on signed_payload
			if err := signing.Verify(req.GetSignature(), pubKey); err != nil {
				return nil, err
			}

			// Deserialize signed_payload to get the trusted Request
			trusted, err := signing.ExtractRequest(req.GetSignature())
			if err != nil {
				return nil, fmt.Errorf("extracting signed request: %w", err)
			}

			// Verify the signed payload matches the wrapper request.
			// The wrapper (minus the signature field) must be structurally
			// identical to the trusted payload. This prevents scope escalation
			// where the wrapper advertises a different operation than what
			// was actually signed.
			wrapperWithoutSig := req.CloneVT()
			wrapperWithoutSig.Signature = nil

			if !wrapperWithoutSig.EqualVT(trusted) {
				// Guardrail: no trusted client in the Antithesis harness ever
				// produces a divergent wrapper/signed_payload pair (the only
				// signing workload signs the exact request it ships). Reaching
				// this branch therefore means either client-side corruption or
				// a serialization bug upstream of the gate — both worth
				// flagging. The rejection itself is the system working; the
				// assertion documents that the divergent-input class is never
				// observed in a trusted-client environment.
				assert.Unreachable("signed payload diverges from wrapper request", map[string]any{
					"keyId": req.GetSignature().GetKeyId(),
				})

				return nil, fmt.Errorf("%w: signed payload does not match wrapper request", signing.ErrInvalidSignature)
			}

			// Attach the original signature to the trusted request for propagation
			trusted.Signature = req.GetSignature()
			result[i] = trusted
		} else {
			// No signature — apply bootstrap rules
			if isSigningManagementRequest(req) {
				// Bootstrap: allow unsigned RegisterSigningKey when no keys exist
				if isRegisterSigningKeyRequest(req) && !a.keyStore.HasKeys() {
					result[i] = req

					continue
				}
				// Keys exist — signing management requires a signature
				return nil, signing.ErrMissingSignature
			}
			// Regular request — check requireSignatures flag
			if a.sharedState.RequireSignatures() {
				return nil, signing.ErrMissingSignature
			}

			result[i] = req
		}
	}

	return result, nil
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
func (errIdempotencyKeyTooLong) Kind() domain.ErrorKind      { return domain.KindValidation }
func (errIdempotencyKeyTooLong) Reason() string              { return domain.ErrReasonValidation }
func (errIdempotencyKeyTooLong) Metadata() map[string]string { return nil }

var ErrIdempotencyKeyTooLong domain.Describable = errIdempotencyKeyTooLong{}

// ErrIdempotencyKeyInvalidUTF8 is returned when an idempotency key contains invalid UTF-8.
type errIdempotencyKeyInvalidUTF8 struct{}

func (errIdempotencyKeyInvalidUTF8) Error() string               { return "idempotency key contains invalid UTF-8" }
func (errIdempotencyKeyInvalidUTF8) Kind() domain.ErrorKind      { return domain.KindValidation }
func (errIdempotencyKeyInvalidUTF8) Reason() string              { return domain.ErrReasonValidation }
func (errIdempotencyKeyInvalidUTF8) Metadata() map[string]string { return nil }

var ErrIdempotencyKeyInvalidUTF8 domain.Describable = errIdempotencyKeyInvalidUTF8{}

// ErrMaintenanceMode is returned when maintenance mode is active and the request is not a maintenance mode toggle.
// Distinct from domain.ErrMaintenanceMode (FSM-level): this one is admission-level (caller hit the gate before the
// proposal entered the Raft pipeline) and shares the same Kind/Reason wire contract.
var ErrMaintenanceMode = domain.ErrMaintenanceMode

// ErrCheckpointOrderNotLast is returned when a bulk request mixes a checkpoint
// trigger (CreateQueryCheckpoint or ClosePeriod) with any non-trigger order
// AND the trigger does not occupy the last slot. The FSM commits the batch as
// a single atomic unit, so a trigger order must always be last — otherwise it
// would force a mid-batch commit that races the pipelined committer.
type errCheckpointOrderNotLast struct{}

func (errCheckpointOrderNotLast) Error() string {
	return "checkpoint trigger (CreateQueryCheckpoint or ClosePeriod) must be the last order in a bulk request"
}
func (errCheckpointOrderNotLast) Kind() domain.ErrorKind      { return domain.KindValidation }
func (errCheckpointOrderNotLast) Reason() string              { return domain.ErrReasonValidation }
func (errCheckpointOrderNotLast) Metadata() map[string]string { return nil }

var ErrCheckpointOrderNotLast domain.Describable = errCheckpointOrderNotLast{}

// allRequestsAreMaintenanceMode returns true if every request in the batch is a SetMaintenanceMode request.
func allRequestsAreMaintenanceMode(requests []*servicepb.Request) bool {
	for _, req := range requests {
		if _, ok := req.GetType().(*servicepb.Request_SetMaintenanceMode); !ok {
			return false
		}
	}

	return true
}

// addVolumeNeed adds a volume key to the preload needs.
func addVolumeNeed(p *preload.Needs, ledgerID uint32, account, asset string) {
	p.Volumes[domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
		Asset:      asset,
	}] = struct{}{}
}

// addTransactionTargetNeeds preloads the right entry for a TargetTransaction.
// When the identifier carries an id, the corresponding TransactionState is
// preloaded so the FSM can read it from cache. When it carries a reference,
// only the reference entry is preloaded — the FSM resolves it against the
// WriteSet, which sees both committed references (via this preload) and
// references just written by an earlier order in the same batch.
func addTransactionTargetNeeds(p *preload.Needs, ledgerID uint32, target *commonpb.TargetTransaction) {
	switch id := target.GetIdentifier().(type) {
	case *commonpb.TargetTransaction_Id:
		p.Transactions[domain.TransactionKey{
			LedgerID: ledgerID,
			ID:       id.Id,
		}] = struct{}{}
	case *commonpb.TargetTransaction_Reference:
		if id.Reference == "" {
			return
		}

		p.References[domain.TransactionReferenceKey{
			LedgerID:  ledgerID,
			Reference: id.Reference,
		}] = struct{}{}
	}
}

// resolveLedgerIDs builds a name-to-ID map for all ledger names referenced in orders.
// Ledger names from CreateLedger orders are excluded since the ledger doesn't exist yet.
func (a *Admission) resolveLedgerIDs(orders []*raftcmdpb.Order) (map[string]uint32, error) {
	nameToID := make(map[string]uint32)
	createNames := make(map[string]struct{})

	// First pass: collect all unique ledger names and track CreateLedger names.
	for _, order := range orders {
		switch orderType := order.GetType().(type) {
		case *raftcmdpb.Order_CreateLedger:
			createNames[orderType.CreateLedger.GetName()] = struct{}{}
		case *raftcmdpb.Order_DeleteLedger:
			nameToID[orderType.DeleteLedger.GetName()] = 0
		case *raftcmdpb.Order_MirrorIngest:
			nameToID[orderType.MirrorIngest.GetLedger()] = 0
		case *raftcmdpb.Order_Apply:
			nameToID[orderType.Apply.GetLedger()] = 0
		case *raftcmdpb.Order_CreatePreparedQuery:
			nameToID[orderType.CreatePreparedQuery.GetQuery().GetLedger()] = 0
		case *raftcmdpb.Order_UpdatePreparedQuery:
			nameToID[orderType.UpdatePreparedQuery.GetLedger()] = 0
		case *raftcmdpb.Order_DeletePreparedQuery:
			nameToID[orderType.DeletePreparedQuery.GetLedger()] = 0
		case *raftcmdpb.Order_SaveNumscript:
			nameToID[orderType.SaveNumscript.GetLedger()] = 0
		case *raftcmdpb.Order_DeleteNumscript:
			nameToID[orderType.DeleteNumscript.GetLedger()] = 0
		case *raftcmdpb.Order_SaveLedgerMetadata:
			nameToID[orderType.SaveLedgerMetadata.GetLedger()] = 0
		case *raftcmdpb.Order_DeleteLedgerMetadata:
			nameToID[orderType.DeleteLedgerMetadata.GetLedger()] = 0
		case *raftcmdpb.Order_PromoteLedger:
			nameToID[orderType.PromoteLedger.GetLedger()] = 0
		}
	}

	// Resolve names to IDs. Skip names that are only in CreateLedger or
	// that don't exist yet — the FSM will return the appropriate error.
	for name := range nameToID {
		if _, isCreate := createNames[name]; isCreate {
			continue
		}

		id, ok := a.preloader.ResolveLedgerID(name)
		if !ok {
			// Ledger doesn't exist; skip preload. The FSM will return ErrLedgerNotFound.
			continue
		}

		nameToID[name] = id
	}

	return nameToID, nil
}

// extractPreloadNeeds extracts all preload keys from orders in a single pass.
func (a *Admission) extractPreloadNeeds(ctx context.Context, orders []*raftcmdpb.Order) (*preload.Needs, map[string]uint32, error) {
	p := preload.NewNeeds()

	nameToID, err := a.resolveLedgerIDs(orders)
	if err != nil {
		return nil, nil, err
	}

	for _, order := range orders {
		// Idempotency keys apply to all order types.
		if order.GetIdempotency() != nil && order.GetIdempotency().GetKey() != "" {
			p.IdempotencyKeys[domain.IdempotencyKey{Key: order.GetIdempotency().GetKey()}] = struct{}{}
		}

		switch orderType := order.GetType().(type) {
		case *raftcmdpb.Order_CreateLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.CreateLedger.GetName()}] = struct{}{}
		case *raftcmdpb.Order_DeleteLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.DeleteLedger.GetName()}] = struct{}{}
		case *raftcmdpb.Order_AddEventsSink:
			p.SinkConfigs[domain.SinkConfigKey{Name: orderType.AddEventsSink.GetConfig().GetName()}] = struct{}{}
		case *raftcmdpb.Order_RemoveEventsSink:
			p.SinkConfigs[domain.SinkConfigKey{Name: orderType.RemoveEventsSink.GetName()}] = struct{}{}
		case *raftcmdpb.Order_MirrorIngest:
			p.Ledgers[domain.LedgerKey{Name: orderType.MirrorIngest.GetLedger()}] = struct{}{}
			p.Boundaries[domain.LedgerKey{Name: orderType.MirrorIngest.GetLedger()}] = struct{}{}

			ledgerName := orderType.MirrorIngest.GetLedger()
			ledgerID := nameToID[ledgerName]

			var postings []*commonpb.Posting
			if ct := orderType.MirrorIngest.GetEntry().GetCreatedTransaction(); ct != nil {
				postings = ct.GetPostings()
			} else if rt := orderType.MirrorIngest.GetEntry().GetRevertedTransaction(); rt != nil {
				postings = rt.GetReversePostings()
			}

			for _, posting := range postings {
				addVolumeNeed(p, ledgerID, posting.GetSource(), posting.GetAsset())
				addVolumeNeed(p, ledgerID, posting.GetDestination(), posting.GetAsset())
			}

			// Preload account metadata for previous value capture in logs.
			mi := orderType.MirrorIngest
			if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
				for account, mm := range ct.GetAccountMetadata() {
					for key := range mm.GetValues() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
							Key:        key,
						}] = struct{}{}
					}
				}
			}

			if sm := mi.GetEntry().GetSavedMetadata(); sm != nil {
				if target, ok := sm.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					for key := range sm.GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
							Key:        key,
						}] = struct{}{}
					}
				}
			}

			if dm := mi.GetEntry().GetDeletedMetadata(); dm != nil {
				if target, ok := dm.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
						Key:        dm.GetKey(),
					}] = struct{}{}
				}
			}
		case *raftcmdpb.Order_CreatePreparedQuery:
			ledgerName := orderType.CreatePreparedQuery.GetQuery().GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.PreparedQueries[domain.PreparedQueryKey{LedgerID: nameToID[ledgerName], Name: orderType.CreatePreparedQuery.GetQuery().GetName()}] = struct{}{}
		case *raftcmdpb.Order_UpdatePreparedQuery:
			ledgerName := orderType.UpdatePreparedQuery.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.PreparedQueries[domain.PreparedQueryKey{LedgerID: nameToID[ledgerName], Name: orderType.UpdatePreparedQuery.GetName()}] = struct{}{}
		case *raftcmdpb.Order_DeletePreparedQuery:
			ledgerName := orderType.DeletePreparedQuery.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.PreparedQueries[domain.PreparedQueryKey{LedgerID: nameToID[ledgerName], Name: orderType.DeletePreparedQuery.GetName()}] = struct{}{}
		case *raftcmdpb.Order_PromoteLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.PromoteLedger.GetLedger()}] = struct{}{}
		case *raftcmdpb.Order_SaveNumscript:
			ledgerName := orderType.SaveNumscript.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.NumscriptVersions[domain.NumscriptVersionKey{LedgerID: nameToID[ledgerName], Name: orderType.SaveNumscript.GetName()}] = struct{}{}
			// For semver saves, preload the specific version content for immutability check
			version := orderType.SaveNumscript.GetVersion()
			if version != "" && version != "latest" {
				p.NumscriptContents[domain.NumscriptEntryKey{LedgerID: nameToID[ledgerName], Name: orderType.SaveNumscript.GetName(), Version: version}] = struct{}{}
			}
		case *raftcmdpb.Order_DeleteNumscript:
			ledgerName := orderType.DeleteNumscript.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.NumscriptVersions[domain.NumscriptVersionKey{LedgerID: nameToID[ledgerName], Name: orderType.DeleteNumscript.GetName()}] = struct{}{}
		case *raftcmdpb.Order_Apply:
			ledgerKey := domain.LedgerKey{Name: orderType.Apply.GetLedger()}
			p.Boundaries[ledgerKey] = struct{}{}
			p.Ledgers[ledgerKey] = struct{}{}

			ledgerName := orderType.Apply.GetLedger()
			ledgerID := nameToID[ledgerName]

			switch applyData := orderType.Apply.GetData().(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				if applyData.CreateTransaction.GetReference() != "" {
					p.References[domain.TransactionReferenceKey{
						LedgerID:  ledgerID,
						Reference: applyData.CreateTransaction.GetReference(),
					}] = struct{}{}
				}

				// Scripts (inline or reference) are resolved in a separate pass
				// (resolveScriptsAndEnrichNeeds) after extractPreloadNeeds returns.
				// Skip volume/metadata preload for script-based orders here.
				if applyData.CreateTransaction.GetNumscriptReference() != nil ||
					(applyData.CreateTransaction.GetScript() != nil &&
						applyData.CreateTransaction.GetScript().GetPlain() != "" &&
						len(applyData.CreateTransaction.GetPostings()) == 0) {
					continue
				}

				for _, posting := range applyData.CreateTransaction.GetPostings() {
					addVolumeNeed(p, ledgerID, posting.GetSource(), posting.GetAsset())
					addVolumeNeed(p, ledgerID, posting.GetDestination(), posting.GetAsset())
				}

				// Preload account metadata for previous value capture.
				for account, mm := range applyData.CreateTransaction.GetAccountMetadata() {
					for key := range mm.GetValues() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
							Key:        key,
						}] = struct{}{}
					}
				}

			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				p.Transactions[domain.TransactionKey{
					LedgerID: ledgerID,
					ID:       applyData.RevertTransaction.GetTransactionId(),
				}] = struct{}{}

				for _, posting := range applyData.RevertTransaction.GetOriginalPostings() {
					addVolumeNeed(p, ledgerID, posting.GetDestination(), posting.GetAsset())
					addVolumeNeed(p, ledgerID, posting.GetSource(), posting.GetAsset())
				}

			case *raftcmdpb.LedgerApplyOrder_AddMetadata:
				if target, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					for key := range applyData.AddMetadata.GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
							Key:        key,
						}] = struct{}{}
					}
				}

				if target, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction); ok {
					addTransactionTargetNeeds(p, ledgerID, target.Transaction)
				}

			case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
				if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
						Key:        applyData.DeleteMetadata.GetKey(),
					}] = struct{}{}
				}

				if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction); ok {
					addTransactionTargetNeeds(p, ledgerID, target.Transaction)
				}
			}
		case *raftcmdpb.Order_SaveLedgerMetadata:
			ledgerName := orderType.SaveLedgerMetadata.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}

			for key := range orderType.SaveLedgerMetadata.GetMetadata() {
				p.LedgerMetadata[domain.LedgerMetadataKey{
					LedgerID: nameToID[ledgerName],
					Key:      key,
				}] = struct{}{}
			}
		case *raftcmdpb.Order_DeleteLedgerMetadata:
			ledgerName := orderType.DeleteLedgerMetadata.GetLedger()
			p.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
			p.LedgerMetadata[domain.LedgerMetadataKey{
				LedgerID: nameToID[ledgerName],
				Key:      orderType.DeleteLedgerMetadata.GetKey(),
			}] = struct{}{}
		}
	}

	return p, nameToID, nil
}

// resolveScriptsAndEnrichNeeds resolves ScriptReferences and discovers volume/metadata
// dependencies from all script-based CreateTransaction orders. It enriches the given
// Needs with the discovered dependencies so that a single BuildPreloads call covers everything.
//
// This runs after extractPreloadNeeds (which skips script-based orders) and before BuildPreloads.
func (a *Admission) resolveScriptsAndEnrichNeeds(ctx context.Context, orders []*raftcmdpb.Order, overlay *bulkOverlay, p *preload.Needs, nameToID map[string]uint32) error {
	for _, order := range orders {
		applyOrder, ok := order.GetType().(*raftcmdpb.Order_Apply)
		if !ok {
			continue
		}

		createTx, ok := applyOrder.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction)
		if !ok {
			continue
		}

		ledgerName := applyOrder.Apply.GetLedger()
		ledgerID := nameToID[ledgerName]

		var scriptText string
		var scriptVars map[string]string
		isReference := false

		// Resolve ScriptReference: load numscript content from overlay (intra-bulk) or Pebble.
		var resolvedVersion string

		if ref := createTx.CreateTransaction.GetNumscriptReference(); ref != nil && ref.GetName() != "" {
			content, rv, err := a.resolveNumscriptReference(overlay, ledgerName, ledgerID, ref.GetName(), ref.GetVersion())
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
			ledgerID,
		)
		if err != nil {
			return &domain.BusinessError{Err: &domain.ErrDependencyDiscoveryFailed{Cause: err}}
		}

		if discovered != nil {
			for key := range discovered.SourceVolumes {
				addVolumeNeed(p, key.LedgerID, key.Account, key.Asset)
			}

			for key := range discovered.DestinationVolumes {
				addVolumeNeed(p, key.LedgerID, key.Account, key.Asset)
			}

			for key := range discovered.WrittenMetadata {
				p.Metadata[key] = struct{}{}
			}
		}

		// For references: preload the resolved content keyed by (ledger, name, version).
		// The FSM resolves via NumscriptReference from the dual-gen cache.
		// For inline scripts: the text stays in the order as-is, no preload needed.
		if isReference {
			ref := createTx.CreateTransaction.GetNumscriptReference()
			p.NumscriptContents[domain.NumscriptEntryKey{
				LedgerID: ledgerID,
				Name:     ref.GetName(),
				Version:  resolvedVersion,
			}] = struct{}{}
			// Ensure the order's reference has the resolved version for FSM cache lookup.
			_ = ref
		}
	}

	return nil
}

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order.
func (a *Admission) requestToOrder(ctx context.Context, req *servicepb.Request, overlay *bulkOverlay) (*raftcmdpb.Order, error) {
	order := &raftcmdpb.Order{}

	switch reqType := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:                   reqType.CreateLedger.GetName(),
				InitialSchema:          reqType.CreateLedger.GetInitialSchema(),
				Mode:                   reqType.CreateLedger.GetMode(),
				MirrorSource:           reqType.CreateLedger.GetMirrorSource(),
				AccountTypes:           reqType.CreateLedger.GetAccountTypes(),
				DefaultEnforcementMode: reqType.CreateLedger.GetDefaultEnforcementMode(),
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
				Name: reqType.DeleteLedger.GetName(),
			},
		}
	case *servicepb.Request_Apply:
		applyOrder, err := a.convertApplyRequest(ctx, reqType.Apply)
		if err != nil {
			return nil, err
		}

		order.Type = &raftcmdpb.Order_Apply{
			Apply: applyOrder,
		}
	case *servicepb.Request_RegisterSigningKey:
		var parentKeyID string
		if req.GetSignature() != nil {
			parentKeyID = req.GetSignature().GetKeyId()
		}

		order.Type = &raftcmdpb.Order_RegisterSigningKey{
			RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
				KeyId:       reqType.RegisterSigningKey.GetKeyId(),
				PublicKey:   reqType.RegisterSigningKey.GetPublicKey(),
				ParentKeyId: parentKeyID,
			},
		}
	case *servicepb.Request_RevokeSigningKey:
		order.Type = &raftcmdpb.Order_RevokeSigningKey{
			RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
				KeyId:   reqType.RevokeSigningKey.GetKeyId(),
				Cascade: reqType.RevokeSigningKey.GetCascade(),
			},
		}
	case *servicepb.Request_SetSigningConfig:
		order.Type = &raftcmdpb.Order_SetSigningConfig{
			SetSigningConfig: &raftcmdpb.SetSigningConfigOrder{
				RequireSignatures: reqType.SetSigningConfig.GetRequireSignatures(),
			},
		}
	case *servicepb.Request_AddEventsSink:
		order.Type = &raftcmdpb.Order_AddEventsSink{
			AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
				Config: reqType.AddEventsSink.GetConfig(),
			},
		}

		overlay.sinks.Put(reqType.AddEventsSink.GetConfig().GetName(), reqType.AddEventsSink.GetConfig())
	case *servicepb.Request_RemoveEventsSink:
		order.Type = &raftcmdpb.Order_RemoveEventsSink{
			RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
				Name: reqType.RemoveEventsSink.GetName(),
			},
		}

		overlay.sinks.Delete(reqType.RemoveEventsSink.GetName())
	case *servicepb.Request_ClosePeriod:
		order.Type = &raftcmdpb.Order_ClosePeriod{
			ClosePeriod: &raftcmdpb.ClosePeriodOrder{},
		}
	case *servicepb.Request_SealPeriod:
		order.Type = &raftcmdpb.Order_SealPeriod{
			SealPeriod: &raftcmdpb.SealPeriodOrder{
				PeriodId:    reqType.SealPeriod.GetPeriodId(),
				SealingHash: reqType.SealPeriod.GetSealingHash(),
				StateHash:   reqType.SealPeriod.GetStateHash(),
			},
		}
	case *servicepb.Request_ArchivePeriod:
		if !a.coldStorageEnabled {
			return nil, domain.ErrColdStorageDisabled
		}

		order.Type = &raftcmdpb.Order_ArchivePeriod{
			ArchivePeriod: &raftcmdpb.ArchivePeriodOrder{
				PeriodId: reqType.ArchivePeriod.GetPeriodId(),
			},
		}
	case *servicepb.Request_ConfirmArchivePeriod:
		order.Type = &raftcmdpb.Order_ConfirmArchivePeriod{
			ConfirmArchivePeriod: &raftcmdpb.ConfirmArchivePeriodOrder{
				PeriodId: reqType.ConfirmArchivePeriod.GetPeriodId(),
			},
		}
	case *servicepb.Request_SetMaintenanceMode:
		order.Type = &raftcmdpb.Order_SetMaintenanceMode{
			SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{
				Enabled: reqType.SetMaintenanceMode.GetEnabled(),
			},
		}
	case *servicepb.Request_SetPeriodSchedule:
		order.Type = &raftcmdpb.Order_SetPeriodSchedule{
			SetPeriodSchedule: &raftcmdpb.SetPeriodScheduleOrder{
				Cron: reqType.SetPeriodSchedule.GetCron(),
			},
		}
	case *servicepb.Request_DeletePeriodSchedule:
		order.Type = &raftcmdpb.Order_DeletePeriodSchedule{
			DeletePeriodSchedule: &raftcmdpb.DeletePeriodScheduleOrder{},
		}
	case *servicepb.Request_PromoteLedger:
		order.Type = &raftcmdpb.Order_PromoteLedger{
			PromoteLedger: &raftcmdpb.PromoteLedgerOrder{
				Ledger: reqType.PromoteLedger.GetLedger(),
			},
		}
	case *servicepb.Request_CreatePreparedQuery:
		order.Type = &raftcmdpb.Order_CreatePreparedQuery{
			CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
				Query: reqType.CreatePreparedQuery.GetQuery(),
			},
		}
	case *servicepb.Request_UpdatePreparedQuery:
		order.Type = &raftcmdpb.Order_UpdatePreparedQuery{
			UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{
				Ledger: reqType.UpdatePreparedQuery.GetLedger(),
				Name:   reqType.UpdatePreparedQuery.GetName(),
				Filter: reqType.UpdatePreparedQuery.GetFilter(),
			},
		}
	case *servicepb.Request_DeletePreparedQuery:
		order.Type = &raftcmdpb.Order_DeletePreparedQuery{
			DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{
				Ledger: reqType.DeletePreparedQuery.GetLedger(),
				Name:   reqType.DeletePreparedQuery.GetName(),
			},
		}
	case *servicepb.Request_SetMetadataFieldType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.SetMetadataFieldType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
					SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
						TargetType: reqType.SetMetadataFieldType.GetTargetType(),
						Key:        reqType.SetMetadataFieldType.GetKey(),
						Type:       reqType.SetMetadataFieldType.GetType(),
					},
				},
			},
		}
	case *servicepb.Request_RemoveMetadataFieldType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.RemoveMetadataFieldType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
						TargetType: reqType.RemoveMetadataFieldType.GetTargetType(),
						Key:        reqType.RemoveMetadataFieldType.GetKey(),
					},
				},
			},
		}
	case *servicepb.Request_CreateIndex:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.CreateIndex.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{
					Id: reqType.CreateIndex.GetId(),
				}},
			},
		}
	case *servicepb.Request_DropIndex:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.DropIndex.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_DropIndex{DropIndex: &raftcmdpb.DropIndexOrder{
					Id: reqType.DropIndex.GetId(),
				}},
			},
		}
	case *servicepb.Request_SaveNumscript:
		order.Type = &raftcmdpb.Order_SaveNumscript{
			SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
				Name:    reqType.SaveNumscript.GetName(),
				Content: reqType.SaveNumscript.GetContent(),
				Version: reqType.SaveNumscript.GetVersion(),
				Ledger:  reqType.SaveNumscript.GetLedger(),
			},
		}

		overlay.recordNumscriptSave(
			reqType.SaveNumscript.GetLedger(),
			reqType.SaveNumscript.GetName(),
			reqType.SaveNumscript.GetVersion(),
			reqType.SaveNumscript.GetContent(),
		)
	case *servicepb.Request_DeleteNumscript:
		order.Type = &raftcmdpb.Order_DeleteNumscript{
			DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{
				Name:   reqType.DeleteNumscript.GetName(),
				Ledger: reqType.DeleteNumscript.GetLedger(),
			},
		}

		overlay.recordNumscriptDelete(reqType.DeleteNumscript.GetLedger(), reqType.DeleteNumscript.GetName())
	case *servicepb.Request_CreateQueryCheckpoint:
		order.Type = &raftcmdpb.Order_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
		}
	case *servicepb.Request_DeleteQueryCheckpoint:
		order.Type = &raftcmdpb.Order_DeleteQueryCheckpoint{
			DeleteQueryCheckpoint: &raftcmdpb.DeleteQueryCheckpointOrder{
				CheckpointId: reqType.DeleteQueryCheckpoint.GetCheckpointId(),
			},
		}
	case *servicepb.Request_SetQueryCheckpointSchedule:
		order.Type = &raftcmdpb.Order_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{
				Cron: reqType.SetQueryCheckpointSchedule.GetCron(),
			},
		}
	case *servicepb.Request_DeleteQueryCheckpointSchedule:
		order.Type = &raftcmdpb.Order_DeleteQueryCheckpointSchedule{
			DeleteQueryCheckpointSchedule: &raftcmdpb.DeleteQueryCheckpointScheduleOrder{},
		}
	case *servicepb.Request_AddAccountType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.AddAccountType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
					AddAccountType: &raftcmdpb.AddAccountTypeOrder{
						AccountType: reqType.AddAccountType.GetAccountType(),
					},
				},
			},
		}
	case *servicepb.Request_RemoveAccountType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.RemoveAccountType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
					RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{
						Name: reqType.RemoveAccountType.GetName(),
					},
				},
			},
		}
	case *servicepb.Request_SetDefaultEnforcementMode:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.SetDefaultEnforcementMode.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{
					UpdateDefaultEnforcementMode: &raftcmdpb.UpdateDefaultEnforcementModeOrder{
						EnforcementMode: reqType.SetDefaultEnforcementMode.GetEnforcementMode(),
					},
				},
			},
		}
	case *servicepb.Request_SaveLedgerMetadata:
		order.Type = &raftcmdpb.Order_SaveLedgerMetadata{
			SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
				Ledger:   reqType.SaveLedgerMetadata.GetLedger(),
				Metadata: reqType.SaveLedgerMetadata.GetMetadata(),
			},
		}
	case *servicepb.Request_DeleteLedgerMetadata:
		order.Type = &raftcmdpb.Order_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{
				Ledger: reqType.DeleteLedgerMetadata.GetLedger(),
				Key:    reqType.DeleteLedgerMetadata.GetKey(),
			},
		}
	default:
		return nil, fmt.Errorf("unsupported request type: %T", req.GetType())
	}

	// Validate storage-safety invariants (null bytes in ledger names, metadata keys, etc.)
	if err := validateOrder(order); err != nil {
		return nil, err
	}

	// Set idempotency key if provided (hash will be computed in processor from payload)
	if req.GetIdempotencyKey() != "" {
		if len(req.GetIdempotencyKey()) > maxIdempotencyKeyLength {
			return nil, &domain.BusinessError{Err: ErrIdempotencyKeyTooLong}
		}

		if !utf8.ValidString(req.GetIdempotencyKey()) {
			return nil, &domain.BusinessError{Err: ErrIdempotencyKeyInvalidUTF8}
		}

		order.Idempotency = &commonpb.Idempotency{
			Key: req.GetIdempotencyKey(),
		}
	}

	// Propagate signature for audit trail
	order.Signature = req.GetSignature()

	return order, nil
}

// convertApplyRequest converts a servicepb.LedgerApplyRequest to raftcmdpb.LedgerApplyOrder.
func (a *Admission) convertApplyRequest(ctx context.Context, apply *servicepb.LedgerApplyRequest) (*raftcmdpb.LedgerApplyOrder, error) {
	order := &raftcmdpb.LedgerApplyOrder{
		Ledger: apply.GetLedger(),
	}

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
func (a *Admission) resolveNumscriptReference(overlay *bulkOverlay, ledger string, ledgerID uint32, name, version string) (string, string, error) {
	if content, resolvedVersion, found := a.resolveNumscriptFromOverlay(overlay, ledger, name, version); found {
		return content, resolvedVersion, nil
	}

	if overlay.numscriptLatest.IsDeleted(numscriptNameKey{Ledger: ledger, Name: name}) {
		return "", "", &domain.BusinessError{Err: &domain.ErrNumscriptNotFound{Name: name}}
	}

	nsHandle, handleErr := a.store.NewDirectReadHandle()
	if handleErr != nil {
		return "", "", fmt.Errorf("creating read handle: %w", handleErr)
	}
	defer func() { _ = nsHandle.Close() }()

	info, err := query.ReadNumscript(a.attrs.NumscriptVersion, a.attrs.NumscriptContent, nsHandle, ledgerID, name, version)
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

func (a *Admission) requestsToOrders(ctx context.Context, reqs []*servicepb.Request) ([]*raftcmdpb.Order, *bulkOverlay, error) {
	overlay := newBulkOverlay()
	orders := make([]*raftcmdpb.Order, len(reqs))

	for i, req := range reqs {
		order, err := a.requestToOrder(ctx, req, overlay)
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

// resolveRevertTarget resolves the target transaction id of a Revert action.
// When the payload carries a numeric id, it is returned as-is. When it
// carries a reference, the reference is looked up against the committed
// store (Pebble) — Revert by reference is therefore only supported for
// transactions that have been committed in a previous Raft entry, never for
// transactions created in the current batch.
func (a *Admission) resolveRevertTarget(ctx context.Context, ledgerName string, payload *servicepb.RevertTransactionPayload) (uint64, error) {
	switch identifier := payload.GetIdentifier().(type) {
	case *servicepb.RevertTransactionPayload_TransactionId:
		return identifier.TransactionId, nil
	case *servicepb.RevertTransactionPayload_TransactionReference:
		if identifier.TransactionReference == "" {
			return 0, &domain.BusinessError{Err: domain.ErrTransactionTargetMissing}
		}

		return a.lookupTransactionByReference(ctx, ledgerName, identifier.TransactionReference)
	default:
		return 0, &domain.BusinessError{Err: domain.ErrTransactionTargetMissing}
	}
}

// lookupTransactionByReference resolves a transaction reference against the
// committed primary store. Returns ErrTransactionReferenceNotFound when the
// reference does not exist. The provided context is honoured: if it is
// already cancelled, the lookup is short-circuited before opening a Pebble
// handle.
func (a *Admission) lookupTransactionByReference(ctx context.Context, ledgerName, reference string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ledgerID, ok := a.preloader.ResolveLedgerID(ledgerName)
	if !ok {
		return 0, &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: ledgerName}}
	}

	handle, err := a.store.NewDirectReadHandle()
	if err != nil {
		return 0, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	key := domain.TransactionReferenceKey{LedgerID: ledgerID, Reference: reference}

	value, err := a.attrs.References.Get(handle, key.Bytes())
	if err != nil {
		return 0, fmt.Errorf("reading transaction reference: %w", err)
	}

	if value == nil {
		return 0, &domain.BusinessError{Err: &domain.ErrTransactionReferenceNotFound{Reference: reference}}
	}

	return value.GetTransactionId(), nil
}

// resolveMetadataReferencesAndEnrichNeeds walks SaveMetadata / DeleteMetadata
// orders whose Target.Transaction carries a Reference (rather than an Id) and
// preloads the corresponding TransactionState. If the reference cannot be
// resolved in the committed store (typically because the transaction is being
// created in the same batch), the preload is skipped — the FSM will read the
// state directly from the WriteSet.
func (a *Admission) resolveMetadataReferencesAndEnrichNeeds(ctx context.Context, orders []*raftcmdpb.Order, p *preload.Needs) error {
	for _, order := range orders {
		applyOrder, ok := order.GetType().(*raftcmdpb.Order_Apply)
		if !ok {
			continue
		}

		var target *commonpb.TargetTransaction

		switch data := applyOrder.Apply.GetData().(type) {
		case *raftcmdpb.LedgerApplyOrder_AddMetadata:
			tx, isTx := data.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction)
			if isTx {
				target = tx.Transaction
			}
		case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
			tx, isTx := data.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction)
			if isTx {
				target = tx.Transaction
			}
		}

		if target == nil {
			continue
		}

		ref, isRef := target.GetIdentifier().(*commonpb.TargetTransaction_Reference)
		if !isRef || ref.Reference == "" {
			continue
		}

		ledgerName := applyOrder.Apply.GetLedger()

		txID, err := a.lookupTransactionByReference(ctx, ledgerName, ref.Reference)
		if err != nil {
			// Intra-batch references that have not yet been committed
			// to the store surface as ErrTransactionReferenceNotFound.
			// The FSM resolves them through the WriteSet, so we silently
			// skip the preload — the FSM either succeeds via the
			// WriteSet, or it returns the same NotFound error.
			//
			// ErrLedgerNotFound is handled symmetrically: the targeted
			// ledger may be created in the same batch. resolveLedgerIDs
			// already skips unknown ledgers and defers the decision to
			// the FSM, so we mirror that here.
			var refNotFound *domain.ErrTransactionReferenceNotFound
			var ledgerNotFound *domain.ErrLedgerNotFound
			if errors.As(err, &refNotFound) || errors.As(err, &ledgerNotFound) {
				continue
			}

			return err
		}

		ledgerID, ok := a.preloader.ResolveLedgerID(ledgerName)
		if !ok {
			continue
		}

		p.Transactions[domain.TransactionKey{LedgerID: ledgerID, ID: txID}] = struct{}{}
	}

	return nil
}

// getTransactionPostings retrieves the postings of an original transaction from the store.
// It uses FindTransactionCreationLog to locate the creation log and extract postings.
func (a *Admission) getTransactionPostings(ledgerName string, transactionID uint64) ([]*commonpb.Posting, error) {
	ledgerID, ok := a.preloader.ResolveLedgerID(ledgerName)
	if !ok {
		return nil, &domain.BusinessError{Err: &domain.ErrLedgerNotFound{Name: ledgerName}}
	}

	log, err := query.FindTransactionCreationLog(context.Background(), a.store, a.attrs.Transaction, ledgerID, transactionID)
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
