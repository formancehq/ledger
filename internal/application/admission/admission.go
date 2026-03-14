package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
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
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

var tracer = otel.Tracer("admission")

// marshalBufPool holds reusable buffers for proto.MarshalAppend to avoid
// repeated buffer growth allocations in the proposal hot path.
var marshalBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)

		return &b
	},
}

type Proposer interface {
	Propose(*node.Proposal) (*futures.Future[state.ApplyResult], error)
	InitialIndex() uint64
}

// Admission handles the admission of orders into the Raft cluster.
// It is responsible for preloading volumes and proposing commands.
type Admission struct {
	store          *dal.Store
	logger         logging.Logger
	proposer       Proposer
	healthChecker  health.Checker
	keyStore       *keystore.KeyStore
	sharedState    *state.SharedState
	receiptSigner  *receipt.Signer
	preloader      *preload.Preloader
	attrs          *attributes.Attributes
	numscriptCache *numscript.NumscriptCache

	// Metrics (noop when metricsEnabled is false)
	metricsEnabled            bool
	commandDurationHistogram  metric.Int64Histogram
	commandSizeHistogram      metric.Int64Histogram
	proposeQueueLoadHistogram metric.Int64Histogram
	proposeQueueInflight      atomic.Int32
	proposeQueueFullCounter   metric.Float64Counter
	proposeDurationHistogram  metric.Int64Histogram
	fsmFutureWaitHistogram    metric.Int64Histogram
	preloadDurationHistogram  metric.Int64Histogram
	preloadCounter            metric.Int64Counter
	preloadKeysNeededCounter  metric.Int64Counter
	preloadCacheHitsCounter   metric.Int64Counter
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
	opts ...func(*Admission),
) *Admission {
	a := &Admission{
		store:          store,
		logger:         logger,
		proposer:       proposer,
		preloader:      preloader,
		healthChecker:  healthChecker,
		keyStore:       keyStore,
		sharedState:    sharedState,
		attrs:          attrs,
		numscriptCache: numscriptCache,
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

	// Verify signatures and resolve signed payloads
	ctx, sigSpan := tracer.Start(ctx, "admission.verify_signatures")
	requests, err := a.verifyAndResolveSignatures(requests)

	sigSpan.End()

	if err != nil {
		return nil, err
	}

	// Convert requests to orders
	orders, err := a.requestsToOrders(requests)
	if err != nil {
		return nil, fmt.Errorf("converting requests to orders: %w", err)
	}

	// Step 1: Extract all preload needs from orders in a single pass
	needs := a.extractPreloadNeeds(ctx, orders)

	// Step 2-4: Build preloads via shared Preloader (no lock)
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

	build, err := a.preloader.BuildPreloads(needs)
	if err != nil {
		preloadSpan.End()
		build.ReleaseLoaders(a.preloader.Loaders())

		return nil, fmt.Errorf("building preloads: %w", err)
	}

	cmd.Preload = build.PreloadSet
	preloadSpan.End()

	// Step 5: Marshal outside the proposal lock — the marshal is the slowest
	// part (~µs) and has no dependency on the lock.
	start := time.Now()

	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	proposalData, err := a.marshalCommand(ctx, cmd)
	if err != nil {
		build.ReleaseLoaders(a.preloader.Loaders())

		return nil, err
	}

	// Step 6: Acquire proposal lock and validate boundary.
	updatedPreloads, guard, err := a.preloader.AcquireProposalGuard(build, needs)
	if err != nil {
		if guard != nil {
			guard.ReleaseAll()
		}

		return nil, fmt.Errorf("acquiring proposal guard: %w", err)
	}

	// Rare: boundary shifted — re-marshal with updated preloads under lock.
	if updatedPreloads != nil {
		cmd.Preload = updatedPreloads

		proposalData, err = a.marshalCommand(ctx, cmd)
		if err != nil {
			guard.ReleaseAll()

			return nil, err
		}
	}

	proposal := node.NewProposal(cmd.GetId(), proposalData)

	ctx, proposeSpan := tracer.Start(ctx, "admission.propose")
	proposeStart := time.Now()

	fsmFuture, err := a.proposer.Propose(proposal)
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

// marshalCommand marshals a proposal command into a newly allocated byte slice
// using a pooled buffer. The returned slice is safe for Raft retention.
func (a *Admission) marshalCommand(ctx context.Context, cmd *raftcmdpb.Proposal) ([]byte, error) {
	ctx, marshalSpan := tracer.Start(ctx, "admission.marshal")
	bufp := marshalBufPool.Get().(*[]byte)
	size := cmd.SizeVT()

	buf := *bufp
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := cmd.MarshalToVT(buf)
	if err != nil {
		marshalSpan.End()

		*bufp = buf
		marshalBufPool.Put(bufp)

		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	cmdData := buf[:n]
	a.commandSizeHistogram.Record(ctx, int64(len(cmdData)))

	proposalData := make([]byte, len(cmdData))
	copy(proposalData, cmdData)

	*bufp = buf
	marshalBufPool.Put(bufp)
	marshalSpan.SetAttributes(attribute.Int("command.size_bytes", len(proposalData)))
	marshalSpan.End()

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

// extractPreloadNeeds extracts all preload keys from orders in a single pass.
func (a *Admission) extractPreloadNeeds(ctx context.Context, orders []*raftcmdpb.Order) *preload.Needs {
	p := preload.NewNeeds()

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
			sinkKey := domain.SinkConfigKey{Name: orderType.AddEventsSink.GetConfig().GetName()}
			if p.SinkConfigs == nil {
				p.SinkConfigs = make(map[domain.SinkConfigKey]func() (*commonpb.SinkConfig, error))
			}

			p.SinkConfigs[sinkKey] = func() (*commonpb.SinkConfig, error) {
				return query.ReadSinkConfig(a.store, sinkKey.Name)
			}
		case *raftcmdpb.Order_RemoveEventsSink:
			sinkKey := domain.SinkConfigKey{Name: orderType.RemoveEventsSink.GetName()}
			if p.SinkConfigs == nil {
				p.SinkConfigs = make(map[domain.SinkConfigKey]func() (*commonpb.SinkConfig, error))
			}

			p.SinkConfigs[sinkKey] = func() (*commonpb.SinkConfig, error) {
				return query.ReadSinkConfig(a.store, sinkKey.Name)
			}
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

			for _, posting := range postings {
				p.Volumes[domain.VolumeKey{
					AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
					Asset:      posting.GetAsset(),
				}] = struct{}{}
				p.Volumes[domain.VolumeKey{
					AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
					Asset:      posting.GetAsset(),
				}] = struct{}{}
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
		case *raftcmdpb.Order_PromoteLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.PromoteLedger.GetLedger()}] = struct{}{}
		case *raftcmdpb.Order_SaveNumscript:
			nsKey := domain.NumscriptVersionKey{Name: orderType.SaveNumscript.GetName()}
			if p.NumscriptVersions == nil {
				p.NumscriptVersions = make(map[domain.NumscriptVersionKey]func() (string, error))
			}

			p.NumscriptVersions[nsKey] = func() (string, error) {
				return query.ReadNumscriptLatestVersion(ctx, a.store, nsKey.Name)
			}
			// For semver saves, preload the specific version entry for immutability check
			version := orderType.SaveNumscript.GetVersion()
			if version != "" && version != "latest" {
				entryKey := domain.NumscriptEntryKey{Name: orderType.SaveNumscript.GetName(), Version: version}
				if p.NumscriptEntries == nil {
					p.NumscriptEntries = make(map[domain.NumscriptEntryKey]func() (bool, error))
				}

				p.NumscriptEntries[entryKey] = func() (bool, error) {
					info, err := query.ReadNumscript(ctx, a.store, entryKey.Name, entryKey.Version)
					if err != nil {
						return false, err
					}

					return info != nil, nil
				}
			}
		case *raftcmdpb.Order_DeleteNumscript:
			nsKey := domain.NumscriptVersionKey{Name: orderType.DeleteNumscript.GetName()}
			if p.NumscriptVersions == nil {
				p.NumscriptVersions = make(map[domain.NumscriptVersionKey]func() (string, error))
			}

			p.NumscriptVersions[nsKey] = func() (string, error) {
				return query.ReadNumscriptLatestVersion(ctx, a.store, nsKey.Name)
			}
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

				// Numscript emulation: discover required accounts and metadata
				if applyData.CreateTransaction.GetScript() != nil &&
					applyData.CreateTransaction.GetScript().GetPlain() != "" &&
					len(applyData.CreateTransaction.GetPostings()) == 0 {
					scriptText := applyData.CreateTransaction.GetScript().GetPlain()

					discovered, err := numscript.DiscoverNumscriptDependencies(
						a.numscriptCache,
						scriptText,
						applyData.CreateTransaction.GetScript().GetVars(),
						ledgerName,
					)
					if err != nil {
						a.logger.WithFields(map[string]any{
							"error": err.Error(),
						}).Info("Numscript emulation failed during dependency discovery, skipping preload")
					}

					if discovered != nil {
						for key := range discovered.SourceVolumes {
							p.Volumes[key] = struct{}{}
						}

						for key := range discovered.DestinationVolumes {
							p.Volumes[key] = struct{}{}
						}

						for key := range discovered.Metadata {
							p.Metadata[key] = struct{}{}
						}

						for key := range discovered.WrittenMetadata {
							p.Metadata[key] = struct{}{}
						}
					}

					// Add script text to NumscriptParsed needs for dual-gen cache
					scriptHash := numscript.HashScript(scriptText)
					contentKey := domain.NumscriptContentKey{Hash: scriptHash}

					if p.NumscriptParsed == nil {
						p.NumscriptParsed = make(map[domain.NumscriptContentKey]func() (string, error))
					}

					p.NumscriptParsed[contentKey] = func() (string, error) { return scriptText, nil }

					// Set content_hash and clear plain (FSM resolves from dual-gen cache)
					applyData.CreateTransaction.GetScript().ContentHash = scriptHash[:]
					applyData.CreateTransaction.GetScript().Plain = ""

					continue
				}

				for _, posting := range applyData.CreateTransaction.GetPostings() {
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
				}

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

				for _, posting := range applyData.RevertTransaction.GetOriginalPostings() {
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
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

	return p
}

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order.
func (a *Admission) requestToOrder(req *servicepb.Request) (*raftcmdpb.Order, error) {
	order := &raftcmdpb.Order{}

	switch reqType := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:            reqType.CreateLedger.GetName(),
				InitialSchema:   reqType.CreateLedger.GetInitialSchema(),
				Mode:            reqType.CreateLedger.GetMode(),
				MirrorSource:    reqType.CreateLedger.GetMirrorSource(),
				ChartOfAccounts: reqType.CreateLedger.GetChartOfAccounts(),
				EnforcementMode: reqType.CreateLedger.GetEnforcementMode(),
				AccountTypes:    reqType.CreateLedger.GetAccountTypes(),
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
				Name: reqType.DeleteLedger.GetName(),
			},
		}
	case *servicepb.Request_Apply:
		applyOrder, err := a.convertApplyRequest(reqType.Apply)
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
	case *servicepb.Request_RemoveEventsSink:
		order.Type = &raftcmdpb.Order_RemoveEventsSink{
			RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
				Name: reqType.RemoveEventsSink.GetName(),
			},
		}
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
	case *servicepb.Request_SetAuditConfig:
		order.Type = &raftcmdpb.Order_SetAuditConfig{
			SetAuditConfig: &raftcmdpb.SetAuditConfigOrder{
				Enabled: reqType.SetAuditConfig.GetEnabled(),
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
			},
		}
	case *servicepb.Request_DeleteNumscript:
		order.Type = &raftcmdpb.Order_DeleteNumscript{
			DeleteNumscript: &raftcmdpb.DeleteNumscriptOrder{
				Name: reqType.DeleteNumscript.GetName(),
			},
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
	case *servicepb.Request_UpdateAccountType:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.UpdateAccountType.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_UpdateAccountType{
					UpdateAccountType: &raftcmdpb.UpdateAccountTypeOrder{
						Name:            reqType.UpdateAccountType.GetName(),
						EnforcementMode: reqType.UpdateAccountType.GetEnforcementMode(),
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
	default:
		return nil, fmt.Errorf("unsupported request type: %T", req.GetType())
	}

	// Set idempotency key if provided (hash will be computed in processor from payload)
	if req.GetIdempotencyKey() != "" {
		if len(req.GetIdempotencyKey()) > maxIdempotencyKeyLength {
			return nil, &domain.BusinessError{Err: ErrIdempotencyKeyTooLong}
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
func (a *Admission) convertApplyRequest(apply *servicepb.LedgerApplyRequest) (*raftcmdpb.LedgerApplyOrder, error) {
	order := &raftcmdpb.LedgerApplyOrder{
		Ledger: apply.GetLedger(),
	}

	switch data := apply.GetData().(type) {
	case *servicepb.LedgerApplyRequest_CreateTransaction:
		ct := data.CreateTransaction
		script := ct.GetScript()

		// Resolve ScriptReference: read numscript from Pebble and use it as the script
		if ct.GetScriptReference() != nil {
			if script != nil && script.GetPlain() != "" {
				return nil, &domain.BusinessError{
					Err: domain.ErrScriptAndReferenceConflict,
				}
			}

			info, err := query.ReadNumscript(context.Background(), a.store, ct.GetScriptReference().GetName(), ct.GetScriptReference().GetVersion())
			if err != nil {
				return nil, fmt.Errorf("reading numscript %q: %w", ct.GetScriptReference().GetName(), err)
			}

			if info == nil {
				return nil, &domain.BusinessError{
					Err: &domain.ErrNumscriptNotFound{Name: ct.GetScriptReference().GetName()},
				}
			}

			script = &commonpb.Script{
				Plain: info.GetContent(),
				Vars:  ct.GetScriptReference().GetVars(),
			}
		}

		order.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Postings:        ct.GetPostings(),
				Script:          script,
				Timestamp:       ct.GetTimestamp(),
				Reference:       ct.GetReference(),
				Metadata:        ct.GetMetadata(),
				AccountMetadata: ct.GetAccountMetadata(),
				Force:           ct.GetForce(),
				ExpandVolumes:   ct.GetExpandVolumes(),
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
	case *servicepb.LedgerApplyRequest_UpdateAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_UpdateAccountType{
			UpdateAccountType: &raftcmdpb.UpdateAccountTypeOrder{
				Name:            data.UpdateAccountType.GetName(),
				EnforcementMode: data.UpdateAccountType.GetEnforcementMode(),
			},
		}
	case *servicepb.LedgerApplyRequest_RemoveAccountType:
		order.Data = &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
			RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{
				Name: data.RemoveAccountType.GetName(),
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
func (a *Admission) requestsToOrders(reqs []*servicepb.Request) ([]*raftcmdpb.Order, error) {
	orders := make([]*raftcmdpb.Order, len(reqs))
	for i, req := range reqs {
		order, err := a.requestToOrder(req)
		if err != nil {
			return nil, fmt.Errorf("converting request %d: %w", i, err)
		}

		orders[i] = order
	}

	return orders, nil
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
		return nil, errors.New("log does not contain an apply log")
	}

	switch payload := applyLog.Apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.GetTransaction() == nil {
			return nil, errors.New("invalid log payload: missing transaction")
		}

		return payload.CreatedTransaction.GetTransaction().GetPostings(), nil
	default:
		return nil, errors.New("log does not contain a created transaction")
	}
}
