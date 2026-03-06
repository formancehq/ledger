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
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
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
	cache         *cache.Cache
	store         *dal.Store
	logger        logging.Logger
	proposer      Proposer
	attrs         *attributes.Attributes
	healthChecker health.Checker
	keyStore      *keystore.KeyStore
	sharedState   *state.SharedState
	receiptSigner *receipt.Signer

	nextIndex atomic.Uint64

	// Attribute loaders to avoid duplicate store loads
	loaders *Loaders

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
	cache *cache.Cache,
	store *dal.Store,
	logger logging.Logger,
	proposer Proposer,
	attrs *attributes.Attributes,
	meterProvider metric.MeterProvider,
	healthChecker health.Checker,
	keyStore *keystore.KeyStore,
	sharedState *state.SharedState,
	opts ...func(*Admission),
) *Admission {
	a := &Admission{
		cache:         cache,
		store:         store,
		logger:        logger,
		proposer:      proposer,
		attrs:         attrs,
		healthChecker: healthChecker,
		keyStore:      keyStore,
		sharedState:   sharedState,
		loaders:       NewLoaders(),
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
	a.nextIndex.Store(proposer.InitialIndex())

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
	needs := a.extractPreloadNeeds(orders)

	// Step 2: Read nextIndex atomically (optimistic snapshot for preload boundary).
	nextIndex := a.nextIndex.Load()

	// Step 3: Compute canonical boundary B(nextIndex)
	threshold := a.cache.GenerationThreshold
	boundary := cache.BoundaryIndex(nextIndex, threshold)

	// Step 4: Build preload - track loaded keys for cleanup after command is applied
	loadedKeys := NewLoadedKeysTracker()

	cmd := commands.NewCommand(orders...)
	cmd.Preload.LastPersistedIndex = boundary

	// Preload all data from store/cache
	ctx, preloadSpan := tracer.Start(ctx, "admission.preload",
		trace.WithAttributes(
			attribute.Int("preload.ledgers", len(needs.Ledgers)),
			attribute.Int("preload.boundaries", len(needs.Boundaries)),
			attribute.Int("preload.volumes", len(needs.Volumes)),
			attribute.Int("preload.idempotency_keys", len(needs.IdempotencyKeys)),
			attribute.Int("preload.references", len(needs.References)),
			attribute.Int("preload.sink_configs", len(needs.SinkConfigs)),
			attribute.Int("preload.numscript_versions", len(needs.NumscriptVersions)),
			attribute.Int("preload.numscript_entries", len(needs.NumscriptEntries)),
			attribute.Int("preload.metadata", len(needs.Metadata)),
		))

	// Preload ledgers (for CreateLedger/DeleteLedger orders)
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.Ledgers)),
		metric.WithAttributes(attribute.String("type", "ledgers")))

	for ledgerKey := range needs.Ledgers {
		canonicalKey := ledgerKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.Ledgers.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.Ledgers.LoadOrWait(id, boundary, func() (*commonpb.LedgerInfo, error) {
				return a.attrs.Ledger.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing ledger value at boundary %d for %s: %w", boundary, ledgerKey.Name, err)
			}

			if result.FromLoad {
				loadedKeys.Ledgers = append(loadedKeys.Ledgers, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "ledgers")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "ledgers")))
			}

			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":        id.Hex(),
					"boundary":  boundary,
					"nextIndex": nextIndex,
					"name":      ledgerKey.Name,
					"fromLoad":  result.FromLoad,
				}).Debug("Preloading ledger from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_Ledger{
						Ledger: &raftcmdpb.PreloadLedger{
							Id:   attrID,
							Info: result.Value,
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "ledgers")))
		}
	}

	// Preload boundaries (for Apply orders)
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.Boundaries)),
		metric.WithAttributes(attribute.String("type", "boundaries")))

	for boundaryKey := range needs.Boundaries {
		canonicalKey := boundaryKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.Boundaries.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.Boundaries.LoadOrWait(id, boundary, func() (*raftcmdpb.LedgerBoundaries, error) {
				return a.attrs.Boundary.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing boundary value at boundary %d for %s: %w", boundary, boundaryKey.Name, err)
			}

			if result.FromLoad {
				loadedKeys.Boundaries = append(loadedKeys.Boundaries, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "boundaries")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "boundaries")))
			}

			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":        id.Hex(),
					"boundary":  boundary,
					"nextIndex": nextIndex,
					"name":      boundaryKey.Name,
					"fromLoad":  result.FromLoad,
				}).Debug("Preloading boundary from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_Boundary{
						Boundary: &raftcmdpb.PreloadBoundary{
							Id:         attrID,
							Boundaries: result.Value,
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "boundaries")))
		}
	}

	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.Volumes)),
		metric.WithAttributes(attribute.String("type", "volumes")))

	for volumeKey := range needs.Volumes {
		canonicalKey := volumeKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.Volumes.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.Volumes.LoadOrWait(id, boundary, func() (*raftcmdpb.VolumePair, error) {
				return a.attrs.Volume.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing volume value at boundary %d for %v: %w", boundary, volumeKey, err)
			}

			if result.FromLoad {
				loadedKeys.Volumes = append(loadedKeys.Volumes, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "volumes")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "volumes")))
			}

			attrID := &raftcmdpb.AttributeID{
				Id:  id[:],
				Tag: tag,
			}

			// Extract input/output for the preload message
			var preloadInput, preloadOutput *commonpb.Uint256
			if result.Value != nil {
				preloadInput = result.Value.GetInputKnown()
				preloadOutput = result.Value.GetOutputKnown()
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading volume from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Volume{
					Volume: &raftcmdpb.PreloadVolume{
						Id:     attrID,
						Input:  preloadInput,
						Output: preloadOutput,
					},
				},
			})
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "volumes")))
		}
	}

	// Build preload for idempotency keys not guaranteed in cache
	// Only preload if the key is actually found (has a value), to reduce command size
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.IdempotencyKeys)),
		metric.WithAttributes(attribute.String("type", "idempotency_keys")))

	for ikKey := range needs.IdempotencyKeys {
		canonicalKey := ikKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		// Check IdempotencyKeys cache
		if !a.cache.IdempotencyKeys.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.IdempotencyKeys.LoadOrWait(id, boundary, func() (*commonpb.IdempotencyKeyValue, error) {
				return a.attrs.IdempotencyKeys.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing idempotency key value at boundary %d for key %s: %w", boundary, ikKey.Key, err)
			}

			if result.FromLoad {
				loadedKeys.IdempotencyKeys = append(loadedKeys.IdempotencyKeys, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "idempotency_keys")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "idempotency_keys")))
			}

			// Only send preload if the key exists in the store
			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":          id.Hex(),
					"boundary":    boundary,
					"nextIndex":   nextIndex,
					"key":         ikKey.Key,
					"logSequence": result.Value.GetLogSequence(),
					"fromLoad":    result.FromLoad,
				}).Debug("Preloading idempotency key from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_IdempotencyKey{
						IdempotencyKey: &raftcmdpb.PreloadIdempotencyKey{
							Id:          attrID,
							LogSequence: result.Value.GetLogSequence(),
							Hash:        result.Value.GetHash(),
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "idempotency_keys")))
		}
	}

	// Build preload for transaction references not guaranteed in cache
	// Only preload if the reference exists (has a value), to reduce command size
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.References)),
		metric.WithAttributes(attribute.String("type", "references")))

	for refKey := range needs.References {
		canonicalKey := refKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.References.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.References.LoadOrWait(id, boundary, func() (*commonpb.TransactionReferenceValue, error) {
				return a.attrs.References.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing transaction reference value at boundary %d for ref %s: %w", boundary, refKey.Reference, err)
			}

			if result.FromLoad {
				loadedKeys.References = append(loadedKeys.References, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "references")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "references")))
			}

			// Only send preload if the reference exists in the store
			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":            id.Hex(),
					"boundary":      boundary,
					"nextIndex":     nextIndex,
					"reference":     refKey.Reference,
					"transactionId": result.Value.GetTransactionId(),
					"fromLoad":      result.FromLoad,
				}).Debug("Preloading transaction reference from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_TransactionReference{
						TransactionReference: &raftcmdpb.PreloadTransactionReference{
							Id:            attrID,
							TransactionId: result.Value.GetTransactionId(),
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "references")))
		}
	}

	// Phase 4: Preload sink configs for AddEventsSink/RemoveEventsSink
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.SinkConfigs)),
		metric.WithAttributes(attribute.String("type", "sink_configs")))

	for sinkKey := range needs.SinkConfigs {
		canonicalKey := sinkKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.SinkConfigs.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.SinkConfigs.LoadOrWait(id, boundary, func() (*commonpb.SinkConfig, error) {
				return query.ReadSinkConfig(a.store, sinkKey.Name)
			})
			if err != nil {
				return nil, fmt.Errorf("loading sink config %q from store: %w", sinkKey.Name, err)
			}

			if result.FromLoad {
				loadedKeys.SinkConfigs = append(loadedKeys.SinkConfigs, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "sink_configs")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "sink_configs")))
			}

			// Only send preload if the sink config exists in the store
			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":        id.Hex(),
					"boundary":  boundary,
					"nextIndex": nextIndex,
					"name":      sinkKey.Name,
					"fromLoad":  result.FromLoad,
				}).Debug("Preloading sink config from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_SinkConfig{
						SinkConfig: &raftcmdpb.PreloadSinkConfig{
							Id:     attrID,
							Config: result.Value,
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "sink_configs")))
		}
	}

	// Phase 5: Preload numscript versions for SaveNumscript/DeleteNumscript
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.NumscriptVersions)),
		metric.WithAttributes(attribute.String("type", "numscript_versions")))

	for nsKey := range needs.NumscriptVersions {
		canonicalKey := nsKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.NumscriptVersions.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.NumscriptVersions.LoadOrWait(id, boundary, func() (string, error) {
				return query.ReadNumscriptLatestVersion(ctx, a.store, nsKey.Name)
			})
			if err != nil {
				return nil, fmt.Errorf("loading numscript version for %q from store: %w", nsKey.Name, err)
			}

			if result.FromLoad {
				loadedKeys.NumscriptVersions = append(loadedKeys.NumscriptVersions, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "numscript_versions")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "numscript_versions")))
			}

			// Always send preload (empty string means "not found", which the FSM needs to know)
			attrID := &raftcmdpb.AttributeID{
				Id:  id[:],
				Tag: tag,
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"name":      nsKey.Name,
				"version":   result.Value,
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading numscript version from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_NumscriptVersion{
					NumscriptVersion: &raftcmdpb.PreloadNumscriptVersion{
						Id:      attrID,
						Version: result.Value,
					},
				},
			})
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "numscript_versions")))
		}
	}

	// Phase 6: Preload numscript entries for semver immutability checks
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.NumscriptEntries)),
		metric.WithAttributes(attribute.String("type", "numscript_entries")))

	for entryKey := range needs.NumscriptEntries {
		canonicalKey := entryKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.NumscriptEntries.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.NumscriptEntries.LoadOrWait(id, boundary, func() (bool, error) {
				info, err := query.ReadNumscript(ctx, a.store, entryKey.Name, entryKey.Version)
				if err != nil {
					return false, err
				}

				return info != nil, nil
			})
			if err != nil {
				return nil, fmt.Errorf("loading numscript entry %q v%s from store: %w", entryKey.Name, entryKey.Version, err)
			}

			if result.FromLoad {
				loadedKeys.NumscriptEntries = append(loadedKeys.NumscriptEntries, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "numscript_entries")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "numscript_entries")))
			}

			// Always send preload (both true and false) so the FSM never needs a Pebble read
			attrID := &raftcmdpb.AttributeID{
				Id:  id[:],
				Tag: tag,
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"name":      entryKey.Name,
				"version":   entryKey.Version,
				"exists":    result.Value,
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading numscript entry existence from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_NumscriptEntry{
					NumscriptEntry: &raftcmdpb.PreloadNumscriptEntry{
						Id:     attrID,
						Exists: result.Value,
					},
				},
			})
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "numscript_entries")))
		}
	}

	// Phase 7: Preload account metadata for Numscript meta() calls
	a.preloadKeysNeededCounter.Add(ctx, int64(len(needs.Metadata)),
		metric.WithAttributes(attribute.String("type", "account_metadata")))

	for metadataKey := range needs.Metadata {
		canonicalKey := metadataKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)

		if !a.cache.AccountMetadata.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()

			result, err := a.loaders.AccountMetadata.LoadOrWait(id, boundary, func() (*commonpb.MetadataValue, error) {
				return a.attrs.Metadata.ComputeValue(a.store, boundary, canonicalKey)
			})
			if err != nil {
				return nil, fmt.Errorf("computing account metadata value at boundary %d for %s/%s: %w", boundary, metadataKey.Account, metadataKey.Key, err)
			}

			if result.FromLoad {
				loadedKeys.AccountMetadata = append(loadedKeys.AccountMetadata, id)

				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "account_metadata")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "account_metadata")))
			}

			// Only send preload if the metadata exists in the store
			if result.Value != nil {
				attrID := &raftcmdpb.AttributeID{
					Id:  id[:],
					Tag: tag,
				}

				a.logger.WithFields(map[string]any{
					"id":        id.Hex(),
					"boundary":  boundary,
					"nextIndex": nextIndex,
					"account":   metadataKey.Account,
					"key":       metadataKey.Key,
					"fromLoad":  result.FromLoad,
				}).Debug("Preloading account metadata from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_AccountMetadata{
						AccountMetadata: &raftcmdpb.PreloadAccountMetadata{
							Id:    attrID,
							Value: result.Value,
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "account_metadata")))
		}
	}

	preloadSpan.End()

	// Step 5: Propose command - reacquire lock to serialize proposals
	start := time.Now()

	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	// Marshal into a pooled buffer to avoid repeated growth allocations.
	// Copy to exact-size slice since Raft retains a reference to proposal data.
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

	// Record command size for monitoring memory usage
	a.commandSizeHistogram.Record(ctx, int64(len(cmdData)))

	proposalData := make([]byte, len(cmdData))
	copy(proposalData, cmdData)

	*bufp = buf // preserve grown capacity for future calls
	marshalBufPool.Put(bufp)
	marshalSpan.SetAttributes(attribute.Int("command.size_bytes", len(proposalData)))
	marshalSpan.End()

	proposal := node.NewProposal(cmd.GetId(), proposalData)

	ctx, proposeSpan := tracer.Start(ctx, "admission.propose")
	proposeStart := time.Now()

	fsmFuture, err := a.proposer.Propose(proposal)
	if err != nil {
		proposeSpan.End()
		// Clean up loaded keys on error
		loadedKeys.MarkApplied(a.loaders)
		a.logger.WithFields(map[string]any{
			"channel": "raft.node.propose",
		}).Errorf("Proposal failed: %v", err)
		a.proposeQueueFullCounter.Add(context.Background(), 1)

		return nil, err
	}

	a.nextIndex.Add(1)
	a.proposeQueueLoadHistogram.Record(context.Background(), int64(a.proposeQueueInflight.Add(1)))

	if _, err := proposal.Wait(); err != nil {
		proposeSpan.End()
		// Clean up loaded keys on error
		loadedKeys.MarkApplied(a.loaders)
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

	// Clean up loaded keys after command is applied (or failed)
	// At this point, the cache will have the values, so we can remove them from the loader
	loadedKeys.MarkApplied(a.loaders)

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

// preloadNeeds contains all data extracted from orders in a single pass.
// It combines what was previously split across phase1 (name-based) and phase2
// (ledger-scoped) extraction, now that DAL keys use ledger names directly.
type preloadNeeds struct {
	IdempotencyKeys   map[domain.IdempotencyKey]struct{}
	Ledgers           map[domain.LedgerKey]struct{}
	Boundaries        map[domain.LedgerKey]struct{}
	SinkConfigs       map[domain.SinkConfigKey]struct{}
	NumscriptVersions map[domain.NumscriptVersionKey]struct{}
	NumscriptEntries  map[domain.NumscriptEntryKey]struct{}
	Volumes           map[domain.VolumeKey]struct{}
	Metadata          map[domain.MetadataKey]struct{}
	References        map[domain.TransactionReferenceKey]struct{}
}

// extractPreloadNeeds extracts all preload keys from orders in a single pass.
//
// For every order it collects:
//   - IdempotencyKeys: from orders with an idempotency key set
//   - Ledgers: from CreateLedger/DeleteLedger orders
//   - Boundaries: from Apply orders (to access next_transaction_id/next_log_id)
//   - SinkConfigs: from AddEventsSink/RemoveEventsSink orders
//   - NumscriptVersions: from SaveNumscript/DeleteNumscript orders
//   - NumscriptEntries: from SaveNumscript orders with explicit semver
//
// For Apply orders it additionally collects:
//   - Volumes: accounts that need balance checks or post-commit volume expansion
//   - Metadata: accounts whose metadata is queried (e.g. Numscript meta() calls)
//   - References: transaction references for uniqueness enforcement
//
// Volume preloading rationale:
//   - Sources always need balance checks (Input + Output to compute balance)
//   - Destinations are only preloaded when ExpandVolumes is true (for post-commit volumes)
//   - When Force is true and ExpandVolumes is false, volume preloading is skipped entirely
//   - When Force is true and ExpandVolumes is true, all volumes are preloaded
//   - Metadata preloading is still performed for Force transactions since
//     Numscript scripts may still query metadata via meta() calls
//   - For reverts, postings are reversed: original destination becomes source
//     and original source becomes destination
func (a *Admission) extractPreloadNeeds(orders []*raftcmdpb.Order) preloadNeeds {
	p := preloadNeeds{
		IdempotencyKeys:   make(map[domain.IdempotencyKey]struct{}),
		Ledgers:           make(map[domain.LedgerKey]struct{}),
		Boundaries:        make(map[domain.LedgerKey]struct{}),
		SinkConfigs:       make(map[domain.SinkConfigKey]struct{}),
		NumscriptVersions: make(map[domain.NumscriptVersionKey]struct{}),
		NumscriptEntries:  make(map[domain.NumscriptEntryKey]struct{}),
		Volumes:           make(map[domain.VolumeKey]struct{}),
		Metadata:          make(map[domain.MetadataKey]struct{}),
		References:        make(map[domain.TransactionReferenceKey]struct{}),
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
		case *raftcmdpb.Order_PromoteLedger:
			p.Ledgers[domain.LedgerKey{Name: orderType.PromoteLedger.GetLedger()}] = struct{}{}
		case *raftcmdpb.Order_SaveNumscript:
			p.NumscriptVersions[domain.NumscriptVersionKey{Name: orderType.SaveNumscript.GetName()}] = struct{}{}
			// For semver saves, preload the specific version entry for immutability check
			version := orderType.SaveNumscript.GetVersion()
			if version != "" && version != "latest" {
				p.NumscriptEntries[domain.NumscriptEntryKey{Name: orderType.SaveNumscript.GetName(), Version: version}] = struct{}{}
			}
		case *raftcmdpb.Order_DeleteNumscript:
			p.NumscriptVersions[domain.NumscriptVersionKey{Name: orderType.DeleteNumscript.GetName()}] = struct{}{}
		case *raftcmdpb.Order_Apply:
			p.Boundaries[domain.LedgerKey{Name: orderType.Apply.GetLedger()}] = struct{}{}

			ledgerName := orderType.Apply.GetLedger()

			switch applyData := orderType.Apply.GetData().(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				// References (extracted regardless of Force or Numscript)
				if applyData.CreateTransaction.GetReference() != "" {
					p.References[domain.TransactionReferenceKey{
						Ledger:    ledgerName,
						Reference: applyData.CreateTransaction.GetReference(),
					}] = struct{}{}
				}

				// Numscript emulation: discover required accounts and metadata by running with infinite balances.
				// This is needed because Numscript transactions have no explicit postings at admission
				// time -- the accounts are determined dynamically by the script at runtime.
				if applyData.CreateTransaction.GetScript() != nil &&
					applyData.CreateTransaction.GetScript().GetPlain() != "" &&
					len(applyData.CreateTransaction.GetPostings()) == 0 {
					discovered, err := numscript.DiscoverNumscriptDependencies(
						applyData.CreateTransaction.GetScript().GetPlain(),
						applyData.CreateTransaction.GetScript().GetVars(),
						ledgerName,
					)
					if err != nil {
						a.logger.WithFields(map[string]any{
							"error": err.Error(),
						}).Info("Numscript emulation failed during dependency discovery, skipping preload")
					}

					if discovered != nil {
						expandVolumes := applyData.CreateTransaction.GetExpandVolumes()
						if !applyData.CreateTransaction.GetForce() || expandVolumes {
							for key := range discovered.SourceVolumes {
								p.Volumes[key] = struct{}{}
							}
						}

						if expandVolumes {
							for key := range discovered.DestinationVolumes {
								p.Volumes[key] = struct{}{}
							}
						}

						for key := range discovered.Metadata {
							p.Metadata[key] = struct{}{}
						}
					}

					continue
				}

				expandVolumes := applyData.CreateTransaction.GetExpandVolumes()
				// Skip volume preloading for force transactions without expandVolumes
				if applyData.CreateTransaction.GetForce() && !expandVolumes {
					continue
				}

				for _, posting := range applyData.CreateTransaction.GetPostings() {
					// Source account needs balance check (or volume expansion)
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
					if expandVolumes {
						// Destination account needed for post-commit volumes
						p.Volumes[domain.VolumeKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
							Asset:      posting.GetAsset(),
						}] = struct{}{}
					}
				}

			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				// Reversion status is checked via in-memory bitset (no preload needed).
				expandVolumes := applyData.RevertTransaction.GetExpandVolumes()
				// Skip volume preloading for force reverts without expandVolumes
				if applyData.RevertTransaction.GetForce() && !expandVolumes {
					continue
				}

				// For reverts, postings are reversed: original destination becomes source (needs balance check)
				for _, posting := range applyData.RevertTransaction.GetOriginalPostings() {
					p.Volumes[domain.VolumeKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetDestination()},
						Asset:      posting.GetAsset(),
					}] = struct{}{}
					if expandVolumes {
						// Original source becomes destination in revert (needed for post-commit volumes)
						p.Volumes[domain.VolumeKey{
							AccountKey: domain.AccountKey{Ledger: ledgerName, Account: posting.GetSource()},
							Asset:      posting.GetAsset(),
						}] = struct{}{}
					}
				}

			case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
				// Preload the metadata key so processDeleteMetadata can check existence
				if target, ok := applyData.DeleteMetadata.GetTarget().GetTarget().(*commonpb.Target_Account); ok {
					p.Metadata[domain.MetadataKey{
						AccountKey: domain.AccountKey{Ledger: ledgerName, Account: target.Account.GetAddr()},
						Key:        applyData.DeleteMetadata.GetKey(),
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
	case *servicepb.Request_SetChartOfAccounts:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.SetChartOfAccounts.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
					SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
						ChartOfAccounts: reqType.SetChartOfAccounts.GetChartOfAccounts(),
					},
				},
			},
		}
	case *servicepb.Request_SetChartEnforcementMode:
		order.Type = &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: reqType.SetChartEnforcementMode.GetLedger(),
				Data: &raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode{
					SetChartEnforcementMode: &raftcmdpb.SetChartEnforcementModeOrder{
						EnforcementMode: reqType.SetChartEnforcementMode.GetEnforcementMode(),
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
				Postings:      ct.GetPostings(),
				Script:        script,
				Timestamp:     ct.GetTimestamp(),
				Reference:     ct.GetReference(),
				Metadata:      ct.GetMetadata(),
				Force:         ct.GetForce(),
				ExpandVolumes: ct.GetExpandVolumes(),
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
	case *servicepb.LedgerApplyRequest_SetChartOfAccounts:
		order.Data = &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
			SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
				ChartOfAccounts: data.SetChartOfAccounts.GetChartOfAccounts(),
			},
		}
	case *servicepb.LedgerApplyRequest_SetChartEnforcementMode:
		order.Data = &raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode{
			SetChartEnforcementMode: &raftcmdpb.SetChartEnforcementModeOrder{
				EnforcementMode: data.SetChartEnforcementMode.GetEnforcementMode(),
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
	log, err := query.FindTransactionCreationLog(context.Background(), a.store, ledgerName, transactionID)
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
