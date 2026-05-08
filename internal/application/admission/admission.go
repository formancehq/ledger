package admission

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/preload"
	"github.com/formancehq/ledger-v3-poc/internal/infra/receipt"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/keystore"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/semver"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/vtmarshal"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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
	needs, err := a.extractPreloadNeeds(ctx, orders)
	if err != nil {
		return nil, err
	}

	// Step 2: Resolve script references and discover script dependencies.
	// This enriches needs with volumes/metadata discovered from scripts.
	if err := a.resolveScriptsAndEnrichNeeds(ctx, orders, overlay, needs); err != nil {
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
	preloadSpan.End()

	// Step 5: Pre-marshal outside the proposal lock.
	//
	// The marshal is the heaviest CPU work in the proposal path, especially
	// for large batches. PredictedIndex is left at zero (omitted in proto3
	// wire format) and will be patched cheaply under the lock via a raw
	// protobuf append — see appendProposalPredictedIndex.
	start := time.Now()

	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	proposalData, err := a.marshalCommand(ctx, cmd)
	if err != nil {
		build.ReleaseLoaders()

		return nil, err
	}

	// Step 6: Acquire proposal lock and validate boundary.
	guardStart := time.Now()
	updatedPreloads, guard, err := a.preloader.AcquireProposalGuard(build, needs)
	if err != nil {
		if guard != nil {
			a.proposalGuardDurationHistogram.Record(ctx, time.Since(guardStart).Microseconds())
			guard.ReleaseAll()
		}

		return nil, fmt.Errorf("acquiring proposal guard: %w", err)
	}

	// Set the predicted index under the proposal guard (tracker locked).
	// The FSM uses this to detect stale proposals whose preloads were
	// computed against an inflated tracker (e.g. after leadership transition).
	cmd.PredictedIndex = a.preloader.TrackerNext()

	if updatedPreloads != nil {
		// Rare path: nextIndex crossed a generation boundary since
		// BuildPreloads, so the preload set was rebuilt under the lock.
		// Must re-marshal the entire command with updated preloads.
		a.proposalGuardRebuildCounter.Add(ctx, 1)
		cmd.Preload = updatedPreloads

		proposalData, err = a.marshalCommand(ctx, cmd)
		if err != nil {
			guard.ReleaseAll()

			return nil, err
		}
	} else {
		// Common path: preloads are still valid. Append only the
		// PredictedIndex field to the pre-marshaled buffer — no full
		// re-marshal needed. See appendProposalPredictedIndex for the
		// protobuf wire-format trick that makes this safe.
		proposalData = appendProposalPredictedIndex(proposalData, cmd.GetPredictedIndex())
	}

	proposal := node.NewProposal(cmd.GetId(), proposalData)

	// Bail out if the caller disconnected (e.g. gRPC client killed).
	// Checking here — right before the Raft proposal — avoids committing
	// entries whose response will never be delivered.
	ctx, proposeSpan := tracer.Start(ctx, "admission.propose")
	proposeStart := time.Now()

	fsmFuture, err := a.proposer.Propose(ctx, proposal)
	if err != nil {
		proposeSpan.End()
		guard.ReleaseAll()
		a.logger.WithFields(map[string]any{
			"channel": "raft.node.propose",
		}).Errorf("Proposal failed: %v", err)
		a.proposeQueueFullCounter.Add(context.Background(), 1)

		return nil, err
	}

	guard.Release()
	a.proposalGuardDurationHistogram.Record(ctx, time.Since(guardStart).Microseconds())
	a.proposeQueueLoadHistogram.Record(context.Background(), int64(a.proposeQueueInflight.Add(1)))

	if _, err := proposal.Wait(); err != nil {
		proposeSpan.End()
		guard.ReleaseLoaders()
		a.proposeQueueInflight.Add(-1)

		return nil, err
	}

	a.proposeDurationHistogram.Record(ctx, time.Since(proposeStart).Microseconds())
	proposeSpan.End()

	// Wait for FSM to apply the command
	ctx, fsmSpan := tracer.Start(ctx, "admission.fsm_wait")
	fsmWaitStart := time.Now()
	result, err := fsmFuture.Wait()
	if err != nil {
		return nil, err
	}

	a.fsmFutureWaitHistogram.Record(ctx, time.Since(fsmWaitStart).Microseconds())
	fsmSpan.End()

	// Decrement inflight counter after command is fully processed
	a.proposeQueueInflight.Add(-1)

	// Clean up loaded keys after command is applied (or failed).
	// At this point, the cache will have the values, so we can remove them from the loader.
	guard.ReleaseLoaders()

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

// appendProposalPredictedIndex appends the raw protobuf wire encoding of
// Proposal.predicted_index (field 7, varint) to an already-marshaled Proposal.
//
// Why this works — proto3 "last value wins":
//
//	In protobuf's wire format, scalar fields may appear more than once in a
//	message. When the decoder encounters duplicates, the last occurrence wins
//	(see https://protobuf.dev/programming-guides/encoding/#last-one-wins).
//	Because PredictedIndex was zero when we pre-marshaled the command (and
//	proto3 omits zero-valued scalars from the wire), the field is absent from
//	the buffer. Appending it at the end makes the decoder see exactly one
//	occurrence — the correct value set under the proposal lock.
//
// Wire layout of the appended bytes:
//
//	[0x38]              — tag: field 7, wire type 0 (varint) = (7 << 3) | 0
//	[varint bytes...]   — base-128 varint encoding of the predicted index
//
// This saves re-marshaling the entire Proposal (which can be megabytes for
// large batches) while holding the proposal lock. The lock is only needed
// to read a stable PredictedIndex from the IndexTracker; the append itself
// is a ~10-byte memcpy with no allocations (MarshalCopy reserves slack).
func appendProposalPredictedIndex(data []byte, index uint64) []byte {
	if index == 0 {
		return data // zero is the proto3 default — already absent from the wire
	}

	// Tag: field 7, wire type 0 (varint).
	data = append(data, 0x38)

	// Varint encoding (identical to protobuf / encoding/binary.PutUvarint).
	for index >= 0x80 {
		data = append(data, byte(index)|0x80)
		index >>= 7
	}

	data = append(data, byte(index))

	return data
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
var ErrIdempotencyKeyTooLong = errors.New("idempotency key exceeds maximum length of 256 characters")

// ErrIdempotencyKeyInvalidUTF8 is returned when an idempotency key contains invalid UTF-8.
var ErrIdempotencyKeyInvalidUTF8 = errors.New("idempotency key contains invalid UTF-8")

// ErrMaintenanceMode is returned when maintenance mode is active and the request is not a maintenance mode toggle.
var ErrMaintenanceMode = errors.New("cluster is in maintenance mode: write operations are blocked")

// allRequestsAreMaintenanceMode returns true if every request in the batch is a SetMaintenanceMode request.
func allRequestsAreMaintenanceMode(requests []*servicepb.Request) bool {
	for _, req := range requests {
		if _, ok := req.GetType().(*servicepb.Request_SetMaintenanceMode); !ok {
			return false
		}
	}

	return true
}

// cachedLedgerInfo returns the LedgerInfo for a ledger, caching it to avoid
// repeated Pebble reads within a single extractPreloadNeeds call.
func (a *Admission) cachedLedgerInfo(ctx context.Context, cache map[string]*commonpb.LedgerInfo, ledgerName string) *commonpb.LedgerInfo {
	if info, ok := cache[ledgerName]; ok {
		return info
	}

	info, err := query.GetLedgerByName(ctx, a.store, ledgerName)
	if err != nil {
		cache[ledgerName] = nil

		return nil
	}

	cache[ledgerName] = info

	return info
}

// cachedCompiledTypes returns compiled account types for a ledger, caching the result.
func (a *Admission) cachedCompiledTypes(ctx context.Context, cache map[string][]accounttype.CompiledType, ledgerInfoCache map[string]*commonpb.LedgerInfo, ledgerName string) []accounttype.CompiledType {
	if compiled, ok := cache[ledgerName]; ok {
		return compiled
	}

	info := a.cachedLedgerInfo(ctx, ledgerInfoCache, ledgerName)
	if info == nil {
		cache[ledgerName] = nil

		return nil
	}

	compiled := accounttype.CompileTypes(info.GetAccountTypes())
	cache[ledgerName] = compiled

	return compiled
}

// addVolumeNeed routes a volume key to either TransientVolumes (zero-initialized preload,
// no Pebble lookup) or Volumes (normal Pebble lookup) based on the account type's persistence.
func addVolumeNeed(p *preload.Needs, ledgerName, account, asset string, compiledTypes []accounttype.CompiledType) {
	vk := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledgerName, Account: account},
		Asset:      asset,
	}

	matched := accounttype.FindMatchingType(account, compiledTypes)
	if matched != nil && matched.GetPersistence() == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
		p.TransientVolumes[vk] = struct{}{}

		return
	}

	p.Volumes[vk] = struct{}{}
}

// extractPreloadNeeds extracts all preload keys from orders in a single pass.
func (a *Admission) extractPreloadNeeds(ctx context.Context, orders []*raftcmdpb.Order) (*preload.Needs, error) {
	p := preload.NewNeeds()

	// Cache LedgerInfo and compiled account types per ledger to avoid repeated Pebble reads.
	ledgerInfoCache := make(map[string]*commonpb.LedgerInfo)
	compiledTypesCache := make(map[string][]accounttype.CompiledType)

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

			var postings []*commonpb.Posting
			if ct := orderType.MirrorIngest.GetEntry().GetCreatedTransaction(); ct != nil {
				postings = ct.GetPostings()
			} else if rt := orderType.MirrorIngest.GetEntry().GetRevertedTransaction(); rt != nil {
				postings = rt.GetReversePostings()
			}

			mirrorTypes := a.cachedCompiledTypes(ctx, compiledTypesCache, ledgerInfoCache, ledgerName)
			for _, posting := range postings {
				addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset(), mirrorTypes)
				addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset(), mirrorTypes)
			}

			// Preload account metadata for previous value capture in logs.
			mi := orderType.MirrorIngest
			if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
				for account, ms := range ct.GetAccountMetadata() {
					for _, md := range ms.GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: account},
							Key:        md.GetKey(),
						}] = struct{}{}
					}
				}
			}

			if sm := mi.GetEntry().GetSavedMetadata(); sm != nil {
				if target, ok := sm.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					for _, entry := range sm.GetMetadata().GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: target.Account.GetAddr()},
							Key:        entry.GetKey(),
						}] = struct{}{}
					}
				}
			}

			if dm := mi.GetEntry().GetDeletedMetadata(); dm != nil {
				if target, ok := dm.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: target.Account.GetAddr()},
						Key:        dm.GetKey(),
					}] = struct{}{}
				}
			}
		case *raftcmdpb.Order_CreatePreparedQuery:
			p.PreparedQueries[domain.PreparedQueryKey{Ledger: orderType.CreatePreparedQuery.GetQuery().GetLedger(), Name: orderType.CreatePreparedQuery.GetQuery().GetName()}] = struct{}{}
		case *raftcmdpb.Order_UpdatePreparedQuery:
			p.PreparedQueries[domain.PreparedQueryKey{Ledger: orderType.UpdatePreparedQuery.GetLedger(), Name: orderType.UpdatePreparedQuery.GetName()}] = struct{}{}
		case *raftcmdpb.Order_DeletePreparedQuery:
			p.PreparedQueries[domain.PreparedQueryKey{Ledger: orderType.DeletePreparedQuery.GetLedger(), Name: orderType.DeletePreparedQuery.GetName()}] = struct{}{}
		case *raftcmdpb.Order_PromoteLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.PromoteLedger.GetLedger()}] = struct{}{}
		case *raftcmdpb.Order_SaveNumscript:
			p.NumscriptVersions[domain.NumscriptVersionKey{Ledger: orderType.SaveNumscript.GetLedger(), Name: orderType.SaveNumscript.GetName()}] = struct{}{}
			// For semver saves, preload the specific version content for immutability check
			version := orderType.SaveNumscript.GetVersion()
			if version != "" && version != "latest" {
				p.NumscriptContents[domain.NumscriptEntryKey{Ledger: orderType.SaveNumscript.GetLedger(), Name: orderType.SaveNumscript.GetName(), Version: version}] = struct{}{}
			}
		case *raftcmdpb.Order_DeleteNumscript:
			p.NumscriptVersions[domain.NumscriptVersionKey{Ledger: orderType.DeleteNumscript.GetLedger(), Name: orderType.DeleteNumscript.GetName()}] = struct{}{}
		case *raftcmdpb.Order_Apply:
			ledgerKey := domain.LedgerKey{Name: orderType.Apply.GetLedger()}
			p.Boundaries[ledgerKey] = struct{}{}
			p.Ledgers[ledgerKey] = struct{}{}

			ledgerName := orderType.Apply.GetLedger()

			switch applyData := orderType.Apply.GetData().(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				if applyData.CreateTransaction.GetReference() != "" {
					p.References[domain.TransactionReferenceKey{
						Ledger:    ledgerName,
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

				txTypes := a.cachedCompiledTypes(ctx, compiledTypesCache, ledgerInfoCache, ledgerName)
				for _, posting := range applyData.CreateTransaction.GetPostings() {
					addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset(), txTypes)
					addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset(), txTypes)
				}

				// Enrich preload with old-address volumes for MIGRATING account types.
				enrichMigratingVolumePreloads(a.cachedLedgerInfo(ctx, ledgerInfoCache, ledgerName), ledgerName, p, volumeKeysFromPostings(ledgerName, applyData.CreateTransaction.GetPostings()))

				// Preload account metadata for previous value capture.
				for account, ms := range applyData.CreateTransaction.GetAccountMetadata() {
					for _, md := range ms.GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: account},
							Key:        md.GetKey(),
						}] = struct{}{}
					}
				}

			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				p.Transactions[domain.TransactionKey{
					Ledger: ledgerName,
					ID:     applyData.RevertTransaction.GetTransactionId(),
				}] = struct{}{}

				revertTypes := a.cachedCompiledTypes(ctx, compiledTypesCache, ledgerInfoCache, ledgerName)
				for _, posting := range applyData.RevertTransaction.GetOriginalPostings() {
					addVolumeNeed(p, ledgerName, posting.GetDestination(), posting.GetAsset(), revertTypes)
					addVolumeNeed(p, ledgerName, posting.GetSource(), posting.GetAsset(), revertTypes)
				}

			case *raftcmdpb.LedgerApplyOrder_AddMetadata:
				if target, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					for _, entry := range applyData.AddMetadata.GetMetadata().GetMetadata() {
						p.Metadata[domain.MetadataKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: target.Account.GetAddr()},
							Key:        entry.GetKey(),
						}] = struct{}{}
					}
				}

				if target, ok := applyData.AddMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction); ok {
					p.Transactions[domain.TransactionKey{
						Ledger: ledgerName,
						ID:     target.Transaction.GetId(),
					}] = struct{}{}
				}

			case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
				if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: target.Account.GetAddr()},
						Key:        applyData.DeleteMetadata.GetKey(),
					}] = struct{}{}
				}

				if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Transaction); ok {
					p.Transactions[domain.TransactionKey{
						Ledger: ledgerName,
						ID:     target.Transaction.GetId(),
					}] = struct{}{}
				}
			}
		}
	}

	return p, nil
}

// resolveScriptsAndEnrichNeeds resolves ScriptReferences and discovers volume/metadata
// dependencies from all script-based CreateTransaction orders. It enriches the given
// Needs with the discovered dependencies so that a single BuildPreloads call covers everything.
//
// This runs after extractPreloadNeeds (which skips script-based orders) and before BuildPreloads.
func (a *Admission) resolveScriptsAndEnrichNeeds(ctx context.Context, orders []*raftcmdpb.Order, overlay *bulkOverlay, p *preload.Needs) error {
	ledgerInfoCache := make(map[string]*commonpb.LedgerInfo)
	compiledTypesCache := make(map[string][]accounttype.CompiledType)

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

		var scriptText string
		isReference := false

		// Resolve ScriptReference: load numscript content from overlay (intra-bulk) or Pebble.
		var resolvedVersion string

		if ref := createTx.CreateTransaction.GetNumscriptReference(); ref != nil && ref.GetName() != "" {
			content, rv, err := a.resolveNumscriptReference(overlay, ledgerName, ref.GetName(), ref.GetVersion())
			if err != nil {
				return err
			}

			scriptText = content
			resolvedVersion = rv
			isReference = true

			// Update the order's reference with the resolved version so the FSM
			// uses an exact key for cache lookup.
			ref.Version = resolvedVersion
		} else if createTx.CreateTransaction.GetScript() != nil &&
			createTx.CreateTransaction.GetScript().GetPlain() != "" &&
			len(createTx.CreateTransaction.GetPostings()) == 0 {
			// Inline script
			scriptText = createTx.CreateTransaction.GetScript().GetPlain()
		} else {
			// Postings-only — handled by extractPreloadNeeds
			continue
		}

		discovered, err := numscript.DiscoverNumscriptDependencies(
			a.numscriptCache,
			scriptText,
			createTx.CreateTransaction.GetScript().GetVars(),
			ledgerName,
		)
		if err != nil {
			if errors.Is(err, numscript.ErrMetaNotSupported) {
				return &domain.BusinessError{Err: numscript.ErrMetaNotSupported}
			}

			if a.logger.Enabled(logging.DebugLevel) {
				a.logger.WithFields(map[string]any{
					"error": err.Error(),
				}).Debug("Numscript emulation failed during dependency discovery, skipping preload")
			}
		}

		if discovered != nil {
			scriptTypes := a.cachedCompiledTypes(ctx, compiledTypesCache, ledgerInfoCache, ledgerName)

			for key := range discovered.SourceVolumes {
				addVolumeNeed(p, key.Ledger, key.Account, key.Asset, scriptTypes)
			}

			for key := range discovered.DestinationVolumes {
				addVolumeNeed(p, key.Ledger, key.Account, key.Asset, scriptTypes)
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
				Ledger:  ledgerName,
				Name:    ref.GetName(),
				Version: resolvedVersion,
			}] = struct{}{}
			// Ensure the order's reference has the resolved version for FSM cache lookup.
			_ = ref
		}

		// Enrich preload with old-address volumes for MIGRATING account types.
		if discovered != nil {
			var scriptVolKeys []domain.VolumeKey
			for key := range discovered.SourceVolumes {
				scriptVolKeys = append(scriptVolKeys, key)
			}

			for key := range discovered.DestinationVolumes {
				scriptVolKeys = append(scriptVolKeys, key)
			}

			enrichMigratingVolumePreloads(a.cachedLedgerInfo(ctx, ledgerInfoCache, ledgerName), ledgerName, p, scriptVolKeys)
		}
	}

	return nil
}

// volumeKeysFromPostings extracts volume keys from postings (source + destination).
func volumeKeysFromPostings(ledgerName string, postings []*commonpb.Posting) []domain.VolumeKey {
	keys := make([]domain.VolumeKey, 0, len(postings)*2)
	for _, posting := range postings {
		keys = append(keys,
			domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
				Asset:      posting.GetAsset(),
			},
			domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
				Asset:      posting.GetAsset(),
			},
		)
	}

	return keys
}

// enrichMigratingVolumePreloads adds old-address volume keys to the preload needs
// for any address that matches a MIGRATING account type's target pattern.
// This ensures the FSM can combine old+new volumes via cache lookups only.
// The ledgerInfo is passed in to avoid repeated Pebble reads (the caller
// caches it per ledger within extractPreloadNeeds).
func enrichMigratingVolumePreloads(
	ledgerInfo *commonpb.LedgerInfo,
	ledgerName string,
	p *preload.Needs,
	volumeKeys []domain.VolumeKey,
) {
	if len(volumeKeys) == 0 || ledgerInfo == nil {
		return
	}

	for _, at := range ledgerInfo.GetAccountTypes() {
		if at.GetStatus() != commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING || at.GetMigration() == nil {
			continue
		}

		targetSegments, tErr := accounttype.ParsePattern(at.GetMigration().GetTargetPattern())
		if tErr != nil {
			continue
		}

		oldSegments, oErr := accounttype.ParsePattern(at.GetPattern())
		if oErr != nil {
			continue
		}

		for _, vk := range volumeKeys {
			if vk.Account == "world" {
				continue
			}

			bindings, ok := accounttype.MatchAddress(vk.Account, targetSegments)
			if !ok {
				continue
			}

			oldAddr, rwErr := accounttype.RewriteAddress(&bindings, oldSegments)
			if rwErr != nil {
				continue
			}

			p.Volumes[domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: oldAddr},
				Asset:      vk.Asset,
			}] = struct{}{}
		}
	}
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
		createIndexOrder := &raftcmdpb.CreateIndexOrder{}

		switch idx := reqType.CreateIndex.GetIndex().(type) {
		case *servicepb.CreateIndexRequest_Transaction:
			createIndexOrder.Index = &raftcmdpb.CreateIndexOrder_Transaction{Transaction: idx.Transaction}
		case *servicepb.CreateIndexRequest_Account:
			createIndexOrder.Index = &raftcmdpb.CreateIndexOrder_Account{Account: idx.Account}
		case *servicepb.CreateIndexRequest_LogBuiltin:
			createIndexOrder.Index = &raftcmdpb.CreateIndexOrder_LogBuiltin{LogBuiltin: idx.LogBuiltin}
		}

		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.CreateIndex.GetLedger(),
				Data:   &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: createIndexOrder},
			},
		}
	case *servicepb.Request_DropIndex:
		dropIndexOrder := &raftcmdpb.DropIndexOrder{}

		switch idx := reqType.DropIndex.GetIndex().(type) {
		case *servicepb.DropIndexRequest_Transaction:
			dropIndexOrder.Index = &raftcmdpb.DropIndexOrder_Transaction{Transaction: idx.Transaction}
		case *servicepb.DropIndexRequest_Account:
			dropIndexOrder.Index = &raftcmdpb.DropIndexOrder_Account{Account: idx.Account}
		case *servicepb.DropIndexRequest_LogBuiltin:
			dropIndexOrder.Index = &raftcmdpb.DropIndexOrder_LogBuiltin{LogBuiltin: idx.LogBuiltin}
		}

		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.DropIndex.GetLedger(),
				Data:   &raftcmdpb.LedgerApplyOrder_DropIndex{DropIndex: dropIndexOrder},
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
	case *servicepb.Request_MigrateAccountType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.MigrateAccountType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_StartAccountMigration{
					StartAccountMigration: &raftcmdpb.StartAccountMigrationOrder{
						AccountTypeName: reqType.MigrateAccountType.GetName(),
						TargetPattern:   reqType.MigrateAccountType.GetTargetPattern(),
					},
				},
			},
		}
	default:
		return nil, fmt.Errorf("unsupported request type: %T", req.GetType())
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

	switch data := apply.GetData().(type) {
	case *servicepb.LedgerApplyRequest_CreateTransaction:
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
	case *servicepb.LedgerApplyRequest_AddMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
			AddMetadata: &raftcmdpb.SaveMetadataOrder{
				Target:   data.AddMetadata.GetTarget(),
				Metadata: data.AddMetadata.GetMetadata(),
			},
		}
	case *servicepb.LedgerApplyRequest_DeleteMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
			DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
				Target: data.DeleteMetadata.GetTarget(),
				Key:    data.DeleteMetadata.GetKey(),
			},
		}
	case *servicepb.LedgerApplyRequest_AddAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_AddAccountType{
			AddAccountType: &raftcmdpb.AddAccountTypeOrder{
				AccountType: data.AddAccountType.GetAccountType(),
			},
		}
	case *servicepb.LedgerApplyRequest_RemoveAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
			RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{
				Name: data.RemoveAccountType.GetName(),
			},
		}
	case *servicepb.LedgerApplyRequest_SetDefaultEnforcementMode:
		order.Data = &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{
			UpdateDefaultEnforcementMode: &raftcmdpb.UpdateDefaultEnforcementModeOrder{
				EnforcementMode: data.SetDefaultEnforcementMode.GetEnforcementMode(),
			},
		}
	case *servicepb.LedgerApplyRequest_MigrateAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_StartAccountMigration{
			StartAccountMigration: &raftcmdpb.StartAccountMigrationOrder{
				AccountTypeName: data.MigrateAccountType.GetName(),
				TargetPattern:   data.MigrateAccountType.GetTargetPattern(),
			},
		}
	case *servicepb.LedgerApplyRequest_RevertTransaction:
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

			if claims.TxID != data.RevertTransaction.GetTransactionId() {
				return nil, fmt.Errorf("receipt txID %d does not match request txID %d", claims.TxID, data.RevertTransaction.GetTransactionId())
			}

			originalPostings = receipt.ClaimsToPostings(claims.Postings)
		} else {
			// Fall back to reading from Pebble
			var err error

			originalPostings, err = a.getTransactionPostings(apply.GetLedger(), data.RevertTransaction.GetTransactionId())
			if err != nil {
				return nil, fmt.Errorf("getting original transaction postings: %w", err)
			}
		}

		order.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
			RevertTransaction: &raftcmdpb.RevertTransactionOrder{
				TransactionId:    data.RevertTransaction.GetTransactionId(),
				Force:            data.RevertTransaction.GetForce(),
				AtEffectiveDate:  data.RevertTransaction.GetAtEffectiveDate(),
				Metadata:         data.RevertTransaction.GetMetadata(),
				OriginalPostings: originalPostings,
				ExpandVolumes:    data.RevertTransaction.GetExpandVolumes(),
			},
		}
	default:
		return nil, fmt.Errorf("unsupported apply data type: %T", apply.GetData())
	}

	return order, nil
}

// requestsToOrders converts a slice of servicepb.Request to raftcmdpb.Order.
// resolveNumscriptReference resolves a numscript reference from the overlay (intra-bulk) or Pebble.
func (a *Admission) resolveNumscriptReference(overlay *bulkOverlay, ledger, name, version string) (string, string, error) {
	if content, resolvedVersion, found := a.resolveNumscriptFromOverlay(overlay, ledger, name, version); found {
		return content, resolvedVersion, nil
	}

	if overlay.numscriptLatest.IsDeleted(numscriptNameKey{Ledger: ledger, Name: name}) {
		return "", "", &domain.BusinessError{Err: &domain.ErrNumscriptNotFound{Name: name}}
	}

	info, err := query.ReadNumscript(a.attrs.NumscriptVersion, a.attrs.NumscriptContent, a.store, ledger, name, version)
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

	return orders, overlay, nil
}

// getTransactionPostings retrieves the postings of an original transaction from the store.
// It uses FindTransactionCreationLog to locate the creation log and extract postings.
func (a *Admission) getTransactionPostings(ledgerName string, transactionID uint64) ([]*commonpb.Posting, error) {
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
