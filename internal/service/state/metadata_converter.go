package state

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source=metadata_converter.go -destination=metadata_converter_generated_test.go -package=state -mock_names=Proposer=MockProposer

// errConversionAborted is returned from the streaming callback when the field
// is no longer in CONVERTING state mid-batch. It signals an early exit from the
// ForEachInPrefix iteration without being treated as a real error.
var errConversionAborted = fmt.Errorf("conversion aborted: field no longer converting")

// Proposer proposes Raft commands to the cluster.
// Implemented by a thin adapter around *node.Node that serializes orders into a
// raftcmdpb.Proposal, marshals them, and calls Node.Propose.
type Proposer interface {
	ProposeOrders(orders ...*raftcmdpb.Order) error
}

// MetadataConvertRequest contains the data needed to convert existing metadata
// values to a declared type. The ledger is validated lazily by the
// MetadataConverter (not on the Raft hot path).
type MetadataConvertRequest struct {
	LedgerName string
	TargetType commonpb.TargetType
	Key        string
	Type       commonpb.MetadataType
}

// MetadataConverter runs in the background to convert existing metadata values
// when a metadata field type is declared (SetMetadataFieldType).
//
// Incoming requests are drained from requestCh immediately (no back-pressure on
// the FSM) and queued internally. A fixed-size worker pool (poolSize) processes
// them concurrently. When all workers are busy, excess requests accumulate in
// the internal queue and are dispatched as workers become available.
//
// Only the leader node performs the conversion and proposes. Followers wait and
// re-check until the field is no longer in CONVERTING state (completed by the
// leader through Raft).
type MetadataConverter struct {
	logger    logging.Logger
	dataStore *dal.Store
	attrs     *attributes.Attributes
	requestCh chan MetadataConvertRequest
	proposer  Proposer
	isLeader  func() bool
	batchSize int
	poolSize  int
	stopCh    chan struct{}
	doneCh    chan struct{}
	wg        sync.WaitGroup
}

// NewMetadataConverter creates a new background metadata converter.
// poolSize controls the maximum number of concurrent field conversions.
func NewMetadataConverter(
	logger logging.Logger,
	dataStore *dal.Store,
	attrs *attributes.Attributes,
	requestCh chan MetadataConvertRequest,
	proposer Proposer,
	isLeader func() bool,
	batchSize int,
	poolSize int,
) *MetadataConverter {
	if poolSize < 1 {
		poolSize = 1
	}
	return &MetadataConverter{
		logger:    logger.WithFields(map[string]any{"cmp": "metadata-converter"}),
		dataStore: dataStore,
		attrs:     attrs,
		requestCh: requestCh,
		proposer:  proposer,
		isLeader:  isLeader,
		batchSize: batchSize,
		poolSize:  poolSize,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start launches the background metadata conversion goroutine.
func (mc *MetadataConverter) Start() {
	go mc.run()
}

// Stop signals the dispatcher goroutine to stop and waits for all in-flight
// conversions to finish.
func (mc *MetadataConverter) Stop() {
	close(mc.stopCh)
	<-mc.doneCh  // wait for the dispatcher loop to exit
	mc.wg.Wait() // wait for all in-flight conversion workers
}

// run drains requestCh into an internal queue and dispatches work to a bounded
// worker pool. The select loop alternates between:
//   - accepting new requests from requestCh (always, to avoid back-pressure)
//   - dispatching the head of the queue when a pool slot is available
func (mc *MetadataConverter) run() {
	defer close(mc.doneCh)

	sem := make(chan struct{}, mc.poolSize)
	var pending []MetadataConvertRequest

	for {
		// When we have pending work, try to dispatch it alongside accepting
		// new requests and checking for stop.
		if len(pending) > 0 {
			select {
			case <-mc.stopCh:
				return
			case req := <-mc.requestCh:
				pending = append(pending, req)
			case sem <- struct{}{}:
				req := pending[0]
				pending = pending[1:]
				mc.wg.Add(1)
				go func() {
					defer mc.wg.Done()
					defer func() { <-sem }()
					mc.convertWithRetry(req)
				}()
			}
		} else {
			// Nothing pending: just wait for new work or stop.
			select {
			case <-mc.stopCh:
				return
			case req := <-mc.requestCh:
				pending = append(pending, req)
			}
		}
	}
}

// isFieldStillConverting checks whether a metadata field is still in CONVERTING
// state by reading the ledger's metadata schema from the data store.
func (mc *MetadataConverter) isFieldStillConverting(ledgerName string, targetType commonpb.TargetType, key string, expectedType commonpb.MetadataType) bool {
	ledgerInfo, err := query.GetLedgerByName(mc.dataStore, ledgerName)
	if err != nil {
		return false
	}
	if ledgerInfo.MetadataSchema == nil {
		return false
	}
	var fields map[string]*commonpb.MetadataFieldSchema
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = ledgerInfo.MetadataSchema.AccountFields
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = ledgerInfo.MetadataSchema.TransactionFields
	}
	if fields == nil {
		return false
	}
	field, ok := fields[key]
	if !ok {
		return false
	}
	return field.Status == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING && field.Type == expectedType
}

// convertWithRetry retries convert() with exponential backoff until it succeeds
// or the converter is stopped.
// On follower nodes, the loop exits when the field is no longer in CONVERTING
// state (completed by the leader through Raft), without calling the proposers.
func (mc *MetadataConverter) convertWithRetry(req MetadataConvertRequest) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 10 * time.Second

	for {
		// Check if the field is still converting before attempting work.
		// If the leader already completed the conversion, exit early.
		if !mc.isFieldStillConverting(req.LedgerName, req.TargetType, req.Key, req.Type) {
			mc.logger.WithFields(map[string]any{
				"ledger": req.LedgerName,
				"key":    req.Key,
			}).Infof("Field no longer converting (completed by leader), done")
			return
		}

		if !mc.isLeader() {
			mc.logger.WithFields(map[string]any{
				"ledger": req.LedgerName,
				"key":    req.Key,
			}).Infof("Not leader, waiting %v before re-checking", backoff)

			select {
			case <-mc.stopCh:
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		err := mc.convert(req)
		if err == nil {
			return
		}

		if errors.Is(err, errNotLeader) {
			mc.logger.WithFields(map[string]any{
				"ledger": req.LedgerName,
				"key":    req.Key,
			}).Infof("Not leader, waiting %v before re-checking", backoff)
		} else {
			mc.logger.Errorf("Background metadata conversion failed (will retry in %v): %v", backoff, err)
		}

		select {
		case <-mc.stopCh:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// proposeBatch proposes a ConvertMetadataBatch order to Raft.
func (mc *MetadataConverter) proposeBatch(
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
	entries []*raftcmdpb.ConvertMetadataEntry,
	totalKeys uint64,
	convertedKeysSoFar uint64,
) {
	_ = mc.proposer.ProposeOrders(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledgerName,
				Data: &raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch{
					ConvertMetadataBatch: &raftcmdpb.ConvertMetadataBatchOrder{
						TargetType:          targetType,
						Key:                 key,
						ExpectedType:        expectedType,
						Entries:             entries,
						TotalKeys:           totalKeys,
						ConvertedKeysSoFar:  convertedKeysSoFar,
					},
				},
			},
		},
	})
}

// proposeComplete proposes a MetadataConversionComplete order to Raft.
func (mc *MetadataConverter) proposeComplete(
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
) {
	_ = mc.proposer.ProposeOrders(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledgerName,
				Data: &raftcmdpb.LedgerApplyOrder_ConversionComplete{
					ConversionComplete: &raftcmdpb.MetadataConversionCompleteOrder{
						TargetType:   targetType,
						Key:          key,
						ExpectedType: expectedType,
					},
				},
			},
		},
	})
}

// convert scans all account metadata for the specified ledger, finds entries
// matching the declared key, converts values that do not match the expected
// type, and proposes batches of converted entries back through Raft.
//
// Uses two streaming Pebble passes (via ForEachInPrefix) instead of loading all
// entries into memory:
//   - Pass 1: count matching keys for progress tracking (O(1) memory)
//   - Pass 2: convert and propose in batches (O(batch_size) memory)
//
// Both passes use the same point-in-time read snapshot for consistency.
//
// The flow handles both leader and follower nodes:
//   - First, check if the field is still in CONVERTING state. If not, the
//     conversion was already completed through Raft -- exit silently.
//   - If the field is still CONVERTING and this node is not the leader, return
//     errNotLeader so the retry loop waits and re-checks.
//   - Only the leader scans, converts, and proposes.
func (mc *MetadataConverter) convert(req MetadataConvertRequest) error {
	logFields := map[string]any{
		"ledger": req.LedgerName,
		"key":    req.Key,
		"type":   req.Type.String(),
	}

	if !mc.isFieldStillConverting(req.LedgerName, req.TargetType, req.Key, req.Type) {
		mc.logger.WithFields(logFields).Infof("Field no longer converting (completed by leader), done")
		return nil
	}

	if !mc.isLeader() {
		return errNotLeader
	}

	// Validate the ledger exists (off the hot path).
	_, err := query.GetLedgerByName(mc.dataStore, req.LedgerName)
	if err != nil {
		return fmt.Errorf("resolving ledger %q: %w", req.LedgerName, err)
	}

	// Transaction metadata uses read-time enforcement (assembleTransaction replays
	// append-only update logs on every read). No background scan needed — just
	// propose completion to transition CONVERTING → COMPLETE.
	if req.TargetType == commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		mc.proposeComplete(req.LedgerName, req.TargetType, req.Key, req.Type)
		mc.logger.WithFields(logFields).Infof("Transaction metadata conversion complete (read-time enforcement)")
		return nil
	}

	mc.logger.WithFields(logFields).Infof("Starting metadata conversion")

	// Build the canonical prefix for this ledger.
	ledgerPrefix := []byte(req.LedgerName + "\x00")

	// Open a Pebble read handle for a point-in-time snapshot used by both passes.
	reader := mc.dataStore.NewReadHandle()
	defer func() { _ = reader.Close() }()

	// Pass 1: count matching keys for progress tracking (O(1) memory).
	var totalMatchingKeys uint64
	err = mc.attrs.Metadata.ForEachInPrefix(reader, ^uint64(0), ledgerPrefix,
		func(entry attributes.ComputedEntry[*commonpb.MetadataValue]) error {
			var mk domain.MetadataKey
			if err := mk.Unmarshal(entry.CanonicalKey); err != nil {
				return nil // skip unparseable keys
			}
			if mk.Key == req.Key {
				totalMatchingKeys++
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("counting metadata keys for ledger %s: %w", req.LedgerName, err)
	}

	mc.logger.WithFields(map[string]any{
		"ledger":           req.LedgerName,
		"key":              req.Key,
		"totalMatchingKeys": totalMatchingKeys,
	}).Infof("Counted matching metadata keys, starting conversion")

	// Pass 2: convert and propose in batches (O(batch_size) memory).
	batch := make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)
	var convertedSoFar uint64

	err = mc.attrs.Metadata.ForEachInPrefix(reader, ^uint64(0), ledgerPrefix,
		func(entry attributes.ComputedEntry[*commonpb.MetadataValue]) error {
			var mk domain.MetadataKey
			if err := mk.Unmarshal(entry.CanonicalKey); err != nil {
				mc.logger.Errorf("Failed to unmarshal metadata key %x: %v", entry.CanonicalKey, err)
				return nil // skip unparseable keys
			}

			if mk.Key != req.Key {
				return nil
			}

			// Check if the value already matches the expected type.
			if commonpb.TypeMatches(entry.Value, req.Type) {
				convertedSoFar++
				return nil
			}

			// Convert the value to the expected type.
			convertedValue := commonpb.ConvertMetadataValue(entry.Value, req.Type)

			batch = append(batch, &raftcmdpb.ConvertMetadataEntry{
				CanonicalKey:   append([]byte(nil), entry.CanonicalKey...),
				ConvertedValue: convertedValue,
			})

			if len(batch) >= mc.batchSize {
				// Check staleness before proposing each batch.
				if !mc.isFieldStillConverting(req.LedgerName, req.TargetType, req.Key, req.Type) {
					mc.logger.WithFields(logFields).Infof("Field no longer converting mid-batch, aborting")
					return errConversionAborted
				}

				convertedSoFar += uint64(len(batch))
				mc.proposeBatch(req.LedgerName, req.TargetType, req.Key, req.Type, batch, totalMatchingKeys, convertedSoFar)
				batch = make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)
			}

			return nil
		},
	)
	if err != nil {
		if err == errConversionAborted {
			return nil
		}
		return fmt.Errorf("converting metadata for ledger %s: %w", req.LedgerName, err)
	}

	// Propose any remaining partial batch.
	if len(batch) > 0 {
		if !mc.isFieldStillConverting(req.LedgerName, req.TargetType, req.Key, req.Type) {
			mc.logger.WithFields(logFields).Infof("Field no longer converting mid-batch, aborting")
			return nil
		}

		convertedSoFar += uint64(len(batch))
		mc.proposeBatch(req.LedgerName, req.TargetType, req.Key, req.Type, batch, totalMatchingKeys, convertedSoFar)
	}

	// Propose conversion completion.
	mc.proposeComplete(req.LedgerName, req.TargetType, req.Key, req.Type)

	mc.logger.WithFields(logFields).Infof("Metadata conversion complete, proposed completion")

	return nil
}
