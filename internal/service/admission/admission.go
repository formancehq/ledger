package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/commands"
	"github.com/formancehq/ledger-v3-poc/internal/service/futures"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

type Proposer interface {
	Propose(*node.Proposal) (*futures.Future, error)
	InitialIndex() uint64
}

// Admission handles the admission of orders into the Raft cluster.
// It is responsible for preloading volumes and proposing commands.
type Admission struct {
	cache    *cache.Cache
	store    *data.Store
	logger   logging.Logger
	proposer Proposer
	attrs    *attributes.Attributes

	admissionLock sync.Mutex
	nextIndex     uint64

	// Attribute loaders to avoid duplicate store loads
	loaders *Loaders

	// Metrics
	commandDurationHistogram  metric.Int64Histogram
	proposeQueueLoadHistogram metric.Int64Histogram
	proposeQueueInflight      *atomic.Int32
	proposeQueueFullCounter   metric.Float64Counter
}

// NewAdmission creates a new Admission handler.
func NewAdmission(
	cache *cache.Cache,
	store *data.Store,
	logger logging.Logger,
	proposer Proposer,
	attrs *attributes.Attributes,
	commandDurationHistogram metric.Int64Histogram,
	proposeQueueLoadHistogram metric.Int64Histogram,
	proposeQueueInflight *atomic.Int32,
	proposeQueueFullCounter metric.Float64Counter,
) *Admission {
	return &Admission{
		cache:                     cache,
		store:                     store,
		logger:                    logger,
		proposer:                  proposer,
		attrs:                     attrs,
		nextIndex:                 proposer.InitialIndex(),
		loaders:                   NewLoaders(),
		commandDurationHistogram:  commandDurationHistogram,
		proposeQueueLoadHistogram: proposeQueueLoadHistogram,
		proposeQueueInflight:      proposeQueueInflight,
		proposeQueueFullCounter:   proposeQueueFullCounter,
	}
}

// Admit implements the ctrl.Admission interface.
// Preload Strategy for Volumes:
// 1. Volumes transition from store to cache after rotation at index R
// 2. For operations at nextIndex N, a volume V is guaranteed in cache if N > R(V)
// 3. When not guaranteed, load base value from store at boundary B(nextIndex)
// 4. For volumes not guaranteed in cache, load base values from store at B(nextIndex)
// 5. Propose command with Preload containing base values
// TODO: Add a second phase of db lookup as the index can advance quickly and cause a cache miss
func (a *Admission) Admit(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	// Convert requests to orders
	orders, err := a.requestsToOrders(requests)
	if err != nil {
		return nil, fmt.Errorf("converting requests to orders: %w", err)
	}

	// Step 1: Extract required volumes, transactions, and idempotency keys from orders
	neededVolumes := a.extractNeededVolumes(orders)
	neededTransactions := a.extractNeededTransactions(orders)
	neededIdempotencyKeys := a.extractNeededIdempotencyKeys(orders)

	// Step 2: Read nextIndex under lock (don't increment yet)
	a.admissionLock.Lock()
	nextIndex := a.nextIndex
	a.admissionLock.Unlock()

	// Step 3: Compute canonical boundary B(nextIndex)
	threshold := a.cache.GenerationThreshold
	boundary := cache.BoundaryIndex(nextIndex, threshold)

	// Step 4: Build preload for volumes not guaranteed in cache
	// Track loaded keys for cleanup after command is applied
	loadedKeys := NewLoadedKeysTracker()

	cmd := commands.NewCommand(orders...)
	cmd.Preload.LastPersistedIndex = boundary

	for volumeKey := range neededVolumes {
		id, tag := attributes.MakeKey(attributes.DefaultKeys, volumeKey.Bytes())
		attrID := &raftcmdpb.AttributeID{
			Id:  id[:],
			Tag: tag,
		}

		// Check Input cache separately
		if !cache.IsGuaranteed(a.cache.Input, nextIndex, volumeKey.Bytes()) {
			result, err := a.loaders.Input.LoadOrWait(id, boundary, func() (*commonpb.BigInt, error) {
				return a.attrs.Input.ComputeValue(a.store, boundary, id)
			})
			if err != nil {
				return nil, fmt.Errorf("computing input value at boundary %d for %s: %w", boundary, volumeKey, err)
			}

			if result.FromLoad {
				loadedKeys.Input = append(loadedKeys.Input, id)
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"value":     result.Value.Value().String(),
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading input from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Input{
					Input: &raftcmdpb.PreloadInput{
						Id:    attrID,
						Value: result.Value,
					},
				},
			})
		}

		// Check Output cache separately
		if !cache.IsGuaranteed(a.cache.Output, nextIndex, volumeKey.Bytes()) {
			result, err := a.loaders.Output.LoadOrWait(id, boundary, func() (*commonpb.BigInt, error) {
				return a.attrs.Output.ComputeValue(a.store, boundary, id)
			})
			if err != nil {
				return nil, fmt.Errorf("computing output value at boundary %d for %s: %w", boundary, volumeKey, err)
			}

			if result.FromLoad {
				loadedKeys.Output = append(loadedKeys.Output, id)
			}

			a.logger.WithFields(map[string]any{
				"id":        id.Hex(),
				"boundary":  boundary,
				"nextIndex": nextIndex,
				"value":     result.Value.Value().String(),
				"fromLoad":  result.FromLoad,
			}).Debug("Preloading output from store")

			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Output{
					Output: &raftcmdpb.PreloadOutput{
						Id:    attrID,
						Value: result.Value,
					},
				},
			})
		}
	}

	// Build preload for reverted status not guaranteed in cache
	for txKey := range neededTransactions {
		id, tag := attributes.MakeKey(attributes.DefaultKeys, txKey.Bytes())
		attrID := &raftcmdpb.AttributeID{
			Id:  id[:],
			Tag: tag,
		}

		// Check Reversions cache
		if !cache.IsGuaranteed(a.cache.Reversions, nextIndex, txKey.Bytes()) {
			result, err := a.loaders.Reversions.LoadOrWait(id, boundary, func() (bool, error) {
				revertedValue, err := a.attrs.Reverted.ComputeValue(a.store, boundary, id)
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
		}
	}

	// Build preload for idempotency keys not guaranteed in cache
	// Only preload if the key is actually found (has a value), to reduce command size
	for ikKey := range neededIdempotencyKeys {
		id, tag := attributes.MakeKey(attributes.DefaultKeys, ikKey.Bytes())

		// Check IdempotencyKeys cache
		if !cache.IsGuaranteed(a.cache.IdempotencyKeys, nextIndex, ikKey.Bytes()) {
			result, err := a.loaders.IdempotencyKeys.LoadOrWait(id, boundary, func() (*commonpb.IdempotencyKeyValue, error) {
				return a.attrs.IdempotencyKeys.ComputeValue(a.store, boundary, id)
			})
			if err != nil {
				return nil, fmt.Errorf("computing idempotency key value at boundary %d for key %s: %w", boundary, ikKey.Key, err)
			}

			if result.FromLoad {
				loadedKeys.IdempotencyKeys = append(loadedKeys.IdempotencyKeys, id)
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
		}
	}

	// Step 5: Propose command - reacquire lock to serialize proposals
	start := time.Now()
	defer func() {
		a.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	cmdData, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	proposal := node.NewProposal(cmd.Id, cmdData)

	// Reacquire lock before proposing to ensure correct ordering
	a.admissionLock.Lock()
	fsmFuture, err := a.proposer.Propose(proposal)
	if err != nil {
		a.admissionLock.Unlock()
		// Clean up loaded keys on error
		loadedKeys.MarkApplied(a.loaders)
		a.logger.WithFields(map[string]any{
			"channel": "raft.node.propose",
		}).Errorf("Proposal failed: %v", err)
		a.proposeQueueFullCounter.Add(context.Background(), 1)
		return nil, err
	}
	a.nextIndex++
	a.admissionLock.Unlock()
	a.proposeQueueLoadHistogram.Record(context.Background(), int64(a.proposeQueueInflight.Add(1)))

	if _, err := proposal.Wait(); err != nil {
		// Clean up loaded keys on error
		loadedKeys.MarkApplied(a.loaders)
		return nil, err
	}

	// Wait for FSM to apply the command
	logs, err := fsmFuture.Wait()

	// Clean up loaded keys after command is applied (or failed)
	// At this point, the cache will have the values, so we can remove them from the loader
	loadedKeys.MarkApplied(a.loaders)

	return logs, err
}

// extractNeededVolumes extracts all volume keys that are needed for the given orders.
// Both sources and destinations need preloading:
// - Sources need balance checks (Input + Output to compute balance)
// - Destinations need to be in cache to receive credits
func (a *Admission) extractNeededVolumes(orders []*raftcmdpb.Order) map[data.VolumeKey]struct{} {
	neededVolumes := make(map[data.VolumeKey]struct{})

	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			switch applyData := orderType.Apply.Data.(type) {
			case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
				for _, posting := range applyData.CreateTransaction.Postings {
					// Source account needs balance check
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerName: orderType.Apply.Ledger,
							Account:    posting.Source,
						},
						Asset: posting.Asset,
					}] = struct{}{}
					// Destination account needs to be in cache to apply credit
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerName: orderType.Apply.Ledger,
							Account:    posting.Destination,
						},
						Asset: posting.Asset,
					}] = struct{}{}
				}
			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				// For reverts, postings are reversed: original destination becomes source (needs balance check)
				// and original source becomes destination (needs to receive credit)
				for _, posting := range applyData.RevertTransaction.OriginalPostings {
					// Original destination becomes source in revert - needs balance check
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerName: orderType.Apply.Ledger,
							Account:    posting.Destination,
						},
						Asset: posting.Asset,
					}] = struct{}{}
					// Original source becomes destination in revert - needs to receive credit
					neededVolumes[data.VolumeKey{
						AccountKey: data.AccountKey{
							LedgerName: orderType.Apply.Ledger,
							Account:    posting.Source,
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
func (a *Admission) extractNeededTransactions(orders []*raftcmdpb.Order) map[data.TransactionKey]struct{} {
	neededTransactions := make(map[data.TransactionKey]struct{})

	for _, order := range orders {
		switch orderType := order.Type.(type) {
		case *raftcmdpb.Order_Apply:
			switch applyData := orderType.Apply.Data.(type) {
			case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
				// Need to check if the transaction is already reverted
				neededTransactions[data.TransactionKey{
					LedgerName: orderType.Apply.Ledger,
					ID:         applyData.RevertTransaction.TransactionId,
				}] = struct{}{}
			}
		}
	}

	return neededTransactions
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
	// Get all updates for this transaction to find the creation log sequence
	updates, err := a.store.GetTransactionUpdates(ledgerName, transactionID)
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
		return nil, fmt.Errorf("transaction %d not found", transactionID)
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
