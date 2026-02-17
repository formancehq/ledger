package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/health"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/commands"
	"github.com/formancehq/ledger-v3-poc/internal/service/futures"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

// marshalBufPool holds reusable buffers for proto.MarshalAppend to avoid
// repeated buffer growth allocations in the proposal hot path.
var marshalBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

type Proposer interface {
	Propose(*node.Proposal) (*futures.Future, error)
	InitialIndex() uint64
}

// Admission handles the admission of orders into the Raft cluster.
// It is responsible for preloading volumes and proposing commands.
type Admission struct {
	cache         *cache.Cache
	store         *data.Store
	logger        logging.Logger
	proposer      Proposer
	attrs         *attributes.Attributes
	healthChecker health.Checker

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

func NewAdmission(
	cache *cache.Cache,
	store *data.Store,
	logger logging.Logger,
	proposer Proposer,
	attrs *attributes.Attributes,
	meterProvider metric.MeterProvider,
	healthChecker health.Checker,
	opts ...func(*Admission),
) *Admission {
	a := &Admission{
		cache:         cache,
		store:         store,
		logger:        logger,
		proposer:      proposer,
		attrs:         attrs,
		healthChecker: healthChecker,
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
// 5. Propose command with Preload containing base values
func (a *Admission) Admit(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	if !a.healthChecker.IsHealthy() {
		return nil, health.ErrUnhealthy
	}

	// Convert requests to orders
	orders, err := a.requestsToOrders(requests)
	if err != nil {
		return nil, fmt.Errorf("converting requests to orders: %w", err)
	}

	// Step 1: Extract required idempotency keys, references, ledgers, and boundaries from orders (name-based)
	neededIdempotencyKeys := a.extractNeededIdempotencyKeys(orders)
	neededLedgers := a.extractNeededLedgers(orders)
	neededBoundaries := a.extractNeededBoundaries(orders)
	// References are extracted after ledger IDs are resolved (Phase 2.5)

	// Step 2: Read nextIndex atomically (optimistic snapshot for preload boundary).
	nextIndex := a.nextIndex.Load()

	// Step 3: Compute canonical boundary B(nextIndex)
	threshold := a.cache.GenerationThreshold
	boundary := cache.BoundaryIndex(nextIndex, threshold)

	// Step 4: Build preload - track loaded keys for cleanup after command is applied
	loadedKeys := NewLoadedKeysTracker()

	cmd := commands.NewCommand(orders...)
	cmd.Preload.LastPersistedIndex = boundary

	// Phase 1: Preload ledgers first to resolve name→ID mapping
	ledgerIDs := make(map[string]uint32) // ledgerName → ledgerID
	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededLedgers)),
		metric.WithAttributes(attribute.String("type", "ledgers")))
	for ledgerKey := range neededLedgers {
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
				ledgerIDs[ledgerKey.Name] = result.Value.Id
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
			// Ledger is guaranteed in cache — read its ID directly from cache.
			// We cannot use ComputeValue (which reads from PebbleDB at boundary) because
			// for recently created ledgers, the boundary may be before the creation index,
			// causing ComputeValue to return nil even though the ledger exists in cache.
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "ledgers")))
			entry, ok := a.cache.Ledgers.Get(id)
			if ok && entry.Data != nil {
				ledgerIDs[ledgerKey.Name] = entry.Data.Id
			}
		}
	}

	// Phase 1.5: Preload boundaries (for Apply orders) to resolve ledger IDs.
	// Boundaries are always in Gen0 (written on every transaction), so the ledger ID
	// is always available without needing a separate ledger info preload in the hot path.
	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededBoundaries)),
		metric.WithAttributes(attribute.String("type", "boundaries")))
	for boundaryKey := range neededBoundaries {
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
				ledgerIDs[boundaryKey.Name] = result.Value.LedgerId
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
			// Boundary is in cache — extract ledger ID from it
			entry, ok := a.cache.Boundaries.Get(id)
			if ok && entry.Data != nil {
				ledgerIDs[boundaryKey.Name] = entry.Data.LedgerId
			}
		}
	}

	// Phase 2: Extract volumes and transactions using resolved IDs
	neededVolumes := a.extractNeededVolumes(orders, ledgerIDs)
	neededTransactions := a.extractNeededTransactions(orders, ledgerIDs)

	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededVolumes)),
		metric.WithAttributes(attribute.String("type", "volumes")))
	for volumeKey := range neededVolumes {
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
			var preloadInput, preloadOutput *commonpb.BigInt
			if result.Value != nil {
				preloadInput = result.Value.InputKnown
				preloadOutput = result.Value.OutputKnown
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

	// Build preload for reverted status not guaranteed in cache
	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededTransactions)),
		metric.WithAttributes(attribute.String("type", "reversions")))
	for txKey := range neededTransactions {
		canonicalKey := txKey.Bytes()
		id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)
		attrID := &raftcmdpb.AttributeID{
			Id:  id[:],
			Tag: tag,
		}

		// Check Reversions cache
		if !a.cache.Reversions.IsGuaranteedInCache(nextIndex, id) {
			preloadStart := time.Now()
			result, err := a.loaders.Reversions.LoadOrWait(id, boundary, func() (bool, error) {
				revertedValue, err := a.attrs.Reverted.ComputeValue(a.store, boundary, canonicalKey)
				if err != nil {
					return false, err
				}
				if revertedValue != nil {
					return revertedValue.Reverted, nil
				}
				return false, nil
			})
			if err != nil {
				return nil, fmt.Errorf("computing reverted value at boundary %d for tx %d: %w", boundary, txKey.ID, err)
			}

			if result.FromLoad {
				loadedKeys.Reversions = append(loadedKeys.Reversions, id)
				a.preloadDurationHistogram.Record(ctx, time.Since(preloadStart).Microseconds(),
					metric.WithAttributes(attribute.String("type", "reversions")))
				a.preloadCounter.Add(ctx, 1,
					metric.WithAttributes(attribute.String("type", "reversions")))
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"txId":      txKey.ID,
				"reverted":  result.Value,
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading reverted status from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Reverted{
					Reverted: &raftcmdpb.PreloadReverted{
						Id:       attrID,
						Reverted: result.Value,
					},
				},
			})
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "reversions")))
		}
	}

	// Build preload for idempotency keys not guaranteed in cache
	// Only preload if the key is actually found (has a value), to reduce command size
	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededIdempotencyKeys)),
		metric.WithAttributes(attribute.String("type", "idempotency_keys")))
	for ikKey := range neededIdempotencyKeys {
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
					"logSequence": result.Value.LogSequence,
					"fromLoad":    result.FromLoad,
				}).Debug("Preloading idempotency key from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_IdempotencyKey{
						IdempotencyKey: &raftcmdpb.PreloadIdempotencyKey{
							Id:          attrID,
							LogSequence: result.Value.LogSequence,
							Hash:        result.Value.Hash,
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
	neededReferences := a.extractNeededReferences(orders, ledgerIDs)
	a.preloadKeysNeededCounter.Add(ctx, int64(len(neededReferences)),
		metric.WithAttributes(attribute.String("type", "references")))
	for refKey := range neededReferences {
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
					"transactionId": result.Value.TransactionId,
					"fromLoad":      result.FromLoad,
				}).Debug("Preloading transaction reference from store")

				cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
					Type: &raftcmdpb.Preload_TransactionReference{
						TransactionReference: &raftcmdpb.PreloadTransactionReference{
							Id:            attrID,
							TransactionId: result.Value.TransactionId,
						},
					},
				})
			}
		} else {
			a.preloadCacheHitsCounter.Add(ctx, 1,
				metric.WithAttributes(attribute.String("type", "references")))
		}
	}

	// Step 5: Propose command - reacquire lock to serialize proposals
	start := time.Now()
	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	// Marshal into a pooled buffer to avoid repeated growth allocations.
	// Copy to exact-size slice since Raft retains a reference to proposal data.
	bufp := marshalBufPool.Get().(*[]byte)
	cmdData, err := proto.MarshalOptions{}.MarshalAppend((*bufp)[:0], cmd)
	if err != nil {
		*bufp = cmdData
		marshalBufPool.Put(bufp)
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	// Record command size for monitoring memory usage
	a.commandSizeHistogram.Record(ctx, int64(len(cmdData)))

	proposalData := make([]byte, len(cmdData))
	copy(proposalData, cmdData)
	*bufp = cmdData // preserve grown capacity for future calls
	marshalBufPool.Put(bufp)

	proposal := node.NewProposal(cmd.Id, proposalData)

	proposeStart := time.Now()
	fsmFuture, err := a.proposer.Propose(proposal)
	if err != nil {
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
		// Clean up loaded keys on error
		loadedKeys.MarkApplied(a.loaders)
		a.proposeQueueInflight.Add(-1)
		return nil, err
	}
	a.proposeDurationHistogram.Record(ctx, time.Since(proposeStart).Microseconds())

	// Wait for FSM to apply the command
	logs, err := fsmFuture.Wait()

	// Decrement inflight counter after command is fully processed
	a.proposeQueueInflight.Add(-1)

	// Clean up loaded keys after command is applied (or failed)
	// At this point, the cache will have the values, so we can remove them from the loader
	loadedKeys.MarkApplied(a.loaders)

	return logs, err
}

// extractNeededVolumes extracts all volume keys that are needed for the given orders.
// Both sources and destinations need preloading:
// - Sources need balance checks (Input + Output to compute balance)
// - Destinations need to be in cache to receive credits
//
// When Force is true on a CreateTransaction, volume preloading is skipped because:
// - Balance checks are bypassed anyway
// - The processor stores deltas (DiffSinceBaseIndex) instead of absolute values
func (a *Admission) extractNeededVolumes(orders []*raftcmdpb.Order, ledgerIDs map[string]uint32) map[data.VolumeKey]struct{} {
	neededVolumes := make(map[data.VolumeKey]struct{})

	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			ledgerID := ledgerIDs[orderType.Apply.Ledger]
			switch applyData := orderType.Apply.Data.(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				// Skip volume preloading for force transactions - they store deltas only
				if applyData.CreateTransaction.Force {
					continue
				}

				// Numscript emulation: discover required accounts by running with infinite balances.
				// This is needed because Numscript transactions have no explicit postings at admission
				// time — the accounts are determined dynamically by the script at runtime.
				if applyData.CreateTransaction.Script != nil &&
					applyData.CreateTransaction.Script.Plain != "" &&
					len(applyData.CreateTransaction.Postings) == 0 {
					discovered, err := processing.DiscoverNumscriptVolumes(
						applyData.CreateTransaction.Script.Plain,
						applyData.CreateTransaction.Script.Vars,
						ledgerID,
					)
					if err != nil {
						a.logger.WithFields(map[string]any{
							"error": err.Error(),
						}).Info("Numscript emulation failed during volume discovery, skipping preload")
					}
					for key := range discovered {
						neededVolumes[key] = struct{}{}
					}
					continue
				}

				for _, posting := range applyData.CreateTransaction.Postings {
					// Source account needs balance check
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  posting.Source,
						},
						Asset: posting.Asset,
					}] = struct{}{}
					// Destination account needs to be in cache to apply credit
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  posting.Destination,
						},
						Asset: posting.Asset,
					}] = struct{}{}
				}
			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				// For reverts, postings are reversed: original destination becomes source (needs balance check)
				// and original source becomes destination (needs to receive credit)
				// Note: Force on revert only affects balance check, not volume preloading
				// because we need to verify the original transaction exists and is not already reverted
				for _, posting := range applyData.RevertTransaction.OriginalPostings {
					// Original destination becomes source in revert - needs balance check
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  posting.Destination,
						},
						Asset: posting.Asset,
					}] = struct{}{}
					// Original source becomes destination in revert - needs to receive credit
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  posting.Source,
						},
						Asset: posting.Asset,
					}] = struct{}{}
				}
			}
		}
	}

	return neededVolumes
}

// extractNeededTransactions extracts all transaction keys that need their reverted status checked.
// This is needed for revert operations to verify the transaction hasn't already been reverted.
func (a *Admission) extractNeededTransactions(orders []*raftcmdpb.Order, ledgerIDs map[string]uint32) map[data.TransactionKey]struct{} {
	neededTransactions := make(map[data.TransactionKey]struct{})

	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			ledgerID := ledgerIDs[orderType.Apply.Ledger]
			switch applyData := orderType.Apply.Data.(type) {
			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				// Need to check if the transaction is already reverted
				neededTransactions[data.TransactionKey{
					LedgerID: ledgerID,
					ID:       applyData.RevertTransaction.TransactionId,
				}] = struct{}{}
			}
		}
	}

	return neededTransactions
}

// extractNeededLedgers extracts ledger keys that need to be checked for CreateLedger/DeleteLedger orders.
// Apply orders get their ledger IDs from boundaries instead (which are always in Gen0).
func (a *Admission) extractNeededLedgers(orders []*raftcmdpb.Order) map[data.LedgerKey]struct{} {
	needed := make(map[data.LedgerKey]struct{})
	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_CreateLedger:
			needed[data.LedgerKey{Name: orderType.CreateLedger.Name}] = struct{}{}
		case *raftcmdpb.Order_DeleteLedger:
			needed[data.LedgerKey{Name: orderType.DeleteLedger.Name}] = struct{}{}
		}
	}
	return needed
}

// extractNeededBoundaries extracts all boundary keys that need to be preloaded.
// This is needed for apply operations to access next_transaction_id/next_log_id.
func (a *Admission) extractNeededBoundaries(orders []*raftcmdpb.Order) map[data.LedgerKey]struct{} {
	needed := make(map[data.LedgerKey]struct{})
	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			needed[data.LedgerKey{Name: orderType.Apply.Ledger}] = struct{}{}
		}
	}
	return needed
}

// extractNeededReferences extracts all transaction reference keys that need to be checked.
// Only non-empty references from CreateTransactionOrder are included.
func (a *Admission) extractNeededReferences(orders []*raftcmdpb.Order, ledgerIDs map[string]uint32) map[data.TransactionReferenceKey]struct{} {
	neededRefs := make(map[data.TransactionReferenceKey]struct{})

	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			switch applyData := orderType.Apply.Data.(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				if applyData.CreateTransaction.Reference == "" {
					continue
				}
				ledgerID := ledgerIDs[orderType.Apply.Ledger]
				neededRefs[data.TransactionReferenceKey{
					LedgerID:  ledgerID,
					Reference: applyData.CreateTransaction.Reference,
				}] = struct{}{}
			}
		}
	}

	return neededRefs
}

// extractNeededIdempotencyKeys extracts all idempotency keys that need to be checked.
// This is needed to verify if an idempotency key has already been used.
func (a *Admission) extractNeededIdempotencyKeys(orders []*raftcmdpb.Order) map[data.IdempotencyKey]struct{} {
	neededKeys := make(map[data.IdempotencyKey]struct{})

	for _, order := range orders {
		if order.Idempotency == nil || order.Idempotency.Key == "" {
			continue
		}

		neededKeys[data.IdempotencyKey{
			Key: order.Idempotency.Key,
		}] = struct{}{}
	}

	return neededKeys
}

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order
func (a *Admission) requestToOrder(req *servicepb.Request) (*raftcmdpb.Order, error) {
	order := &raftcmdpb.Order{}

	switch reqType := req.Type.(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:     reqType.CreateLedger.Name,
				Metadata: reqType.CreateLedger.Metadata,
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
				Name: reqType.DeleteLedger.Name,
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
	default:
		return nil, fmt.Errorf("unsupported request type: %T", req.Type)
	}

	// Set idempotency key if provided (hash will be computed in processor from payload)
	if req.IdempotencyKey != "" {
		order.Idempotency = &commonpb.Idempotency{
			Key: req.IdempotencyKey,
		}
	}

	return order, nil
}

// convertApplyRequest converts a servicepb.LedgerApplyRequest to raftcmdpb.LedgerApplyOrder
func (a *Admission) convertApplyRequest(apply *servicepb.LedgerApplyRequest) (*raftcmdpb.LedgerApplyOrder, error) {
	order := &raftcmdpb.LedgerApplyOrder{
		Ledger: apply.Ledger,
	}

	switch data := apply.Data.(type) {
	case *servicepb.LedgerApplyRequest_CreateTransaction:
		order.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Postings:  data.CreateTransaction.Postings,
				Script:    data.CreateTransaction.Script,
				Timestamp: data.CreateTransaction.Timestamp,
				Reference: data.CreateTransaction.Reference,
				Metadata:  data.CreateTransaction.Metadata,
				Force:     data.CreateTransaction.Force,
			},
		}
	case *servicepb.LedgerApplyRequest_AddMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
			AddMetadata: &raftcmdpb.SaveMetadataOrder{
				Target:   data.AddMetadata.Target,
				Metadata: data.AddMetadata.Metadata,
			},
		}
	case *servicepb.LedgerApplyRequest_DeleteMetadata:
		order.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
			DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
				Target: data.DeleteMetadata.Target,
				Key:    data.DeleteMetadata.Key,
			},
		}
	case *servicepb.LedgerApplyRequest_RevertTransaction:
		// Fetch original transaction postings from store
		originalPostings, err := a.getTransactionPostings(apply.Ledger, data.RevertTransaction.TransactionId)
		if err != nil {
			return nil, fmt.Errorf("getting original transaction postings: %w", err)
		}
		order.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
			RevertTransaction: &raftcmdpb.RevertTransactionOrder{
				TransactionId:    data.RevertTransaction.TransactionId,
				Force:            data.RevertTransaction.Force,
				AtEffectiveDate:  data.RevertTransaction.AtEffectiveDate,
				Metadata:         data.RevertTransaction.Metadata,
				OriginalPostings: originalPostings,
			},
		}
	default:
		return nil, fmt.Errorf("unsupported apply data type: %T", apply.Data)
	}

	return order, nil
}

// requestsToOrders converts a slice of servicepb.Request to raftcmdpb.Order
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
// It looks up the transaction's creation log to extract the postings.
func (a *Admission) getTransactionPostings(ledgerName string, transactionID uint64) ([]*commonpb.Posting, error) {
	// Resolve ledger name to ID for store queries
	ledgerInfo, err := a.store.GetLedgerByName(ledgerName)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger ID for %s: %w", ledgerName, err)
	}

	// Get all updates for this transaction to find the creation log sequence
	updates, err := a.store.GetTransactionUpdates(ledgerInfo.Id, transactionID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", transactionID, err)
	}

	// Find the sequence (from TransactionInit)
	var sequence uint64
	for _, update := range updates {
		for _, updateType := range update.Updates {
			if updateType.GetTransactionInit() != nil {
				sequence = update.ByLog
				break
			}
		}
		if sequence != 0 {
			break
		}
	}

	if sequence == 0 {
		return nil, &processing.BusinessError{Err: &processing.ErrTransactionNotFound{TransactionID: transactionID}}
	}

	// Get the system log containing the transaction
	log, err := a.store.GetLogBySequence(sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}
	if log == nil {
		return nil, fmt.Errorf("transaction %d not found (log %d missing)", transactionID, sequence)
	}

	// Extract the ledger log from the log
	applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", sequence)
	}
	ledgerLog := applyLog.Apply.Log

	// Extract the postings from the CreatedTransaction payload
	switch payload := ledgerLog.Data.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.Transaction == nil {
			return nil, fmt.Errorf("invalid log payload: missing transaction")
		}
		return payload.CreatedTransaction.Transaction.Postings, nil
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a created transaction", ledgerLog.Id)
	}
}
