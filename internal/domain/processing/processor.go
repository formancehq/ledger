package processing

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing/numscript"
)

type RequestProcessor struct {
	numscriptCache *numscript.NumscriptCache
	logHasher      *blake3.Hasher
	orderHashBuf   []byte // reusable buffer for order hash marshaling
}

// NewRequestProcessor creates a new RequestProcessor with the given meter.
// If meter is nil, a noop meter is used. numscriptCacheSize controls the
// maximum number of parsed scripts kept in the LRU cache (0 = default 1024).
func NewRequestProcessor(m metric.Meter, numscriptCacheSize int) (*RequestProcessor, error) {
	if m == nil {
		m = noop.Meter{}
	}

	cache := numscript.NewNumscriptCache(numscriptCacheSize)
	if err := cache.InitCacheMetrics(m); err != nil {
		return nil, fmt.Errorf("creating numscript cache metrics: %w", err)
	}
	return &RequestProcessor{
		numscriptCache: cache,
		logHasher:      blake3.New(),
		orderHashBuf:   make([]byte, 0, 512),
	}, nil
}

// ProcessOrders processes a list of orders and returns the resulting logs.
func (p *RequestProcessor) ProcessOrders(orders []*raftcmdpb.Order, s InMemoryStore) ([]*raftcmdpb.CreatedLogOrReference, error) {
	logs := make([]*raftcmdpb.CreatedLogOrReference, len(orders))

	for i, order := range orders {
		// Compute idempotency hash once if needed (reused for check + store)
		hasIdempotency := order.Idempotency != nil && order.Idempotency.Key != ""
		var orderHash []byte
		if hasIdempotency {
			orderHash = p.computeOrderHash(order)

			ikKey := domain.IdempotencyKey{Key: order.Idempotency.Key}
			storedValue, err := s.GetIdempotencyKey(ikKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, fmt.Errorf("checking idempotency key: %w", err)
			}

			// Check if idempotency key exists
			if storedValue != nil {
				if !bytes.Equal(orderHash, storedValue.Hash) {
					return nil, &domain.ErrIdempotencyKeyConflict{Key: order.Idempotency.Key}
				}

				// Hash matches - return reference to existing log without processing
				logs[i] = &raftcmdpb.CreatedLogOrReference{
					Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{
						ReferenceSequence: storedValue.LogSequence,
					},
				}
				continue
			}
		}

		// No idempotency key or key not found - process normally
		payload, err := p.ProcessOrder(order, s)
		if err != nil {
			return nil, err
		}

		nextSequenceID := s.IncrementNextSequenceID()
		log := &commonpb.Log{
			Sequence:    nextSequenceID,
			Payload:     payload,
			Idempotency: order.Idempotency,
			Signature:   order.Signature,
		}
		log.Hash = ComputeLogHash(p.logHasher, s.GetLastLogHash(), log)
		s.SetLastLogHash(log.Hash)

		logs[i] = &raftcmdpb.CreatedLogOrReference{
			Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: log,
			},
		}

		// Store idempotency key if present (reuse orderHash computed above)
		if hasIdempotency {
			s.PutIdempotencyKey(
				domain.IdempotencyKey{
					Key: order.Idempotency.Key,
				},
				&commonpb.IdempotencyKeyValue{
					LogSequence: nextSequenceID,
					Hash:        orderHash,
				},
			)
		}
	}

	return logs, nil
}

// computeOrderHash computes a blake3 hash of the order content (excluding
// idempotency) for idempotency checking. It reuses p.orderHashBuf across calls
// to avoid allocations. Safe because ProcessOrders is single-threaded.
func (p *RequestProcessor) computeOrderHash(order *raftcmdpb.Order) []byte {
	// Temporarily nil the idempotency field to exclude it from the hash,
	// then restore it. This avoids a full CloneVT of the order.
	saved := order.Idempotency
	order.Idempotency = nil

	size := order.SizeVT()
	if cap(p.orderHashBuf) < size {
		p.orderHashBuf = make([]byte, size)
	} else {
		p.orderHashBuf = p.orderHashBuf[:size]
	}
	n, err := order.MarshalToVT(p.orderHashBuf)
	order.Idempotency = saved
	if err != nil {
		panic(fmt.Sprintf("failed to marshal order: %v", err))
	}

	hash := blake3.Sum256(p.orderHashBuf[:n])
	return hash[:]
}

// ProcessOrder processes an Order and returns the resulting LogPayload.
func (p *RequestProcessor) ProcessOrder(order *raftcmdpb.Order, s InMemoryStore) (*commonpb.LogPayload, error) {
	switch orderType := order.Type.(type) {
	case *raftcmdpb.Order_Apply:
		return p.processApply(orderType.Apply, s)
	case *raftcmdpb.Order_CreateLedger:
		return p.processCreateLedger(orderType.CreateLedger, s)
	case *raftcmdpb.Order_DeleteLedger:
		return p.processDeleteLedger(orderType.DeleteLedger, s)
	case *raftcmdpb.Order_RegisterSigningKey:
		return p.processRegisterSigningKey(orderType.RegisterSigningKey, s)
	case *raftcmdpb.Order_RevokeSigningKey:
		return p.processRevokeSigningKey(orderType.RevokeSigningKey, s)
	case *raftcmdpb.Order_SetSigningConfig:
		return p.processSetSigningConfig(orderType.SetSigningConfig, s)
	case *raftcmdpb.Order_SetMaintenanceMode:
		return p.processSetMaintenanceMode(orderType.SetMaintenanceMode, s)
	case *raftcmdpb.Order_SetAuditConfig:
		return p.processSetAuditConfig(orderType.SetAuditConfig, s)
	case *raftcmdpb.Order_AddEventsSink:
		return p.processAddEventsSink(orderType.AddEventsSink, s)
	case *raftcmdpb.Order_RemoveEventsSink:
		return p.processRemoveEventsSink(orderType.RemoveEventsSink, s)
	case *raftcmdpb.Order_ClosePeriod:
		return p.processClosePeriod(orderType.ClosePeriod, s)
	case *raftcmdpb.Order_SealPeriod:
		return p.processSealPeriod(orderType.SealPeriod, s)
	case *raftcmdpb.Order_ArchivePeriod:
		return p.processArchivePeriod(orderType.ArchivePeriod, s)
	case *raftcmdpb.Order_ConfirmArchivePeriod:
		return p.processConfirmArchivePeriod(orderType.ConfirmArchivePeriod, s)
	case *raftcmdpb.Order_SetPeriodSchedule:
		return p.processSetPeriodSchedule(orderType.SetPeriodSchedule, s)
	case *raftcmdpb.Order_DeletePeriodSchedule:
		return p.processDeletePeriodSchedule(s)
	case *raftcmdpb.Order_MirrorIngest:
		return p.processMirrorIngest(orderType.MirrorIngest, s)
	case *raftcmdpb.Order_PromoteLedger:
		return p.processPromoteLedger(orderType.PromoteLedger, s)
	case *raftcmdpb.Order_CreatePreparedQuery:
		return p.processCreatePreparedQuery(orderType.CreatePreparedQuery, s)
	case *raftcmdpb.Order_UpdatePreparedQuery:
		return p.processUpdatePreparedQuery(orderType.UpdatePreparedQuery, s)
	case *raftcmdpb.Order_DeletePreparedQuery:
		return p.processDeletePreparedQuery(orderType.DeletePreparedQuery, s)
	case *raftcmdpb.Order_SaveNumscript:
		return p.processSaveNumscript(orderType.SaveNumscript, s)
	case *raftcmdpb.Order_DeleteNumscript:
		return p.processDeleteNumscript(orderType.DeleteNumscript, s)
	default:
		return nil, fmt.Errorf("invalid order type")
	}
}
