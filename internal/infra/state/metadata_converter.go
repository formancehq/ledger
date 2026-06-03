package state

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source=metadata_converter.go -destination=metadata_converter_generated_test.go -package=state -mock_names=Proposer=MockProposer

// Proposer proposes Raft commands to the cluster.
// Implemented by a thin adapter around *node.Node that serializes a
// raftcmdpb.Proposal and calls Node.Propose.
type Proposer interface {
	ProposeProposal(cmd *raftcmdpb.Proposal) error
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
// metadataConvertReconcileInterval is the interval at which the MetadataConverter
// re-checks for CONVERTING fields that may have been missed due to dropped signals.
const metadataConvertReconcileInterval = 30 * time.Second

type MetadataConverter struct {
	logger      logging.Logger
	dataStore   *dal.Store
	attrs       *attributes.Attributes
	requestCh   *worker.Channel[MetadataConvertRequest]
	proposer    Proposer
	isLeader    func() bool
	reconcileFn func(stop <-chan struct{})
	batchSize   int
	poolSize    int
	w           worker.Worker
	wg          sync.WaitGroup
}

// NewMetadataConverter creates a new background metadata converter.
// poolSize controls the maximum number of concurrent field conversions.
// reconcileFn re-dispatches CONVERTING fields from durable state to the channel.
func NewMetadataConverter(
	logger logging.Logger,
	dataStore *dal.Store,
	attrs *attributes.Attributes,
	requestCh *worker.Channel[MetadataConvertRequest],
	proposer Proposer,
	isLeader func() bool,
	batchSize int,
	poolSize int,
	reconcileFn func(stop <-chan struct{}),
) *MetadataConverter {
	if poolSize < 1 {
		poolSize = 1
	}

	return &MetadataConverter{
		logger:      logger.WithFields(map[string]any{"cmp": "metadata-converter"}),
		dataStore:   dataStore,
		attrs:       attrs,
		requestCh:   requestCh,
		proposer:    proposer,
		isLeader:    isLeader,
		reconcileFn: reconcileFn,
		batchSize:   batchSize,
		poolSize:    poolSize,
		w:           worker.New(),
	}
}

// Start launches the background metadata conversion goroutine with periodic reconciliation.
func (mc *MetadataConverter) Start() {
	// Periodic reconciliation: re-scan for CONVERTING fields.
	go func() {
		stop := mc.w.StopCh()
		worker.RunTicker(stop, metadataConvertReconcileInterval, func() {
			if mc.isLeader() {
				mc.reconcileFn(stop)
			}
		})
	}()

	mc.w.Run(mc.dispatchLoop)
}

// Stop signals the dispatcher goroutine to stop and waits for all in-flight
// conversions to finish.
func (mc *MetadataConverter) Stop() {
	mc.w.Stop()
	mc.wg.Wait()
}

// dispatchLoop drains requestCh into an internal queue and dispatches work to a
// bounded worker pool. The select loop alternates between:
//   - accepting new requests from requestCh (always, to avoid back-pressure)
//   - dispatching the head of the queue when a pool slot is available
func (mc *MetadataConverter) dispatchLoop(stop <-chan struct{}) {
	ctx := worker.ContextFromStop(stop)
	sem := make(chan struct{}, mc.poolSize)

	var pending []MetadataConvertRequest

	for {
		// When we have pending work, try to dispatch it alongside accepting
		// new requests and checking for stop.
		if len(pending) > 0 {
			select {
			case <-stop:
				return
			case req := <-mc.requestCh.Receive():
				pending = append(pending, req)
			case sem <- struct{}{}:
				req := pending[0]
				pending = pending[1:]

				mc.wg.Go(func() {
					defer func() { <-sem }()

					mc.convertWithRetry(ctx, stop, req)
				})
			}
		} else {
			// Nothing pending: just wait for new work or stop.
			select {
			case <-stop:
				return
			case req := <-mc.requestCh.Receive():
				pending = append(pending, req)
			}
		}
	}
}

// isFieldStillConverting checks whether a metadata field is still in CONVERTING
// state by reading the ledger's metadata schema from the data store.
func (mc *MetadataConverter) isFieldStillConverting(ctx context.Context, ledgerName string, targetType commonpb.TargetType, key string, expectedType commonpb.MetadataType) bool {
	ledgerInfo, err := query.GetLedgerByName(ctx, mc.dataStore, ledgerName)
	if err != nil {
		return false
	}

	if ledgerInfo.GetMetadataSchema() == nil {
		return false
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = ledgerInfo.GetMetadataSchema().GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = ledgerInfo.GetMetadataSchema().GetTransactionFields()
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		fields = ledgerInfo.GetMetadataSchema().GetLedgerFields()
	}

	if fields == nil {
		return false
	}

	field, ok := fields[key]
	if !ok {
		return false
	}

	return field.GetStatus() == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING && field.GetType() == expectedType
}

// convertWithRetry retries convert() with exponential backoff until it succeeds
// or the converter is stopped.
// On follower nodes, the loop exits when the field is no longer in CONVERTING
// state (completed by the leader through Raft), without calling the proposers.
func (mc *MetadataConverter) convertWithRetry(ctx context.Context, stop <-chan struct{}, req MetadataConvertRequest) {
	worker.RetryWithBackoff(stop, mc.logger, func() error {
		// Check if the field is still converting before attempting work.
		// If the leader already completed the conversion, exit early.
		if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
			mc.logger.WithFields(map[string]any{
				"ledger": req.LedgerName,
				"key":    req.Key,
			}).Infof("Field no longer converting (completed by leader), done")

			return nil
		}

		if !mc.isLeader() {
			return worker.ErrNotLeader
		}

		return mc.convert(ctx, req)
	})
}

// proposeBatch proposes a MetadataConversionBatch to Raft as a direct Proposal field.
func (mc *MetadataConverter) proposeBatch(
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
	entries []*raftcmdpb.ConvertMetadataEntry,
	totalKeys uint64,
	convertedKeysSoFar uint64,
) {
	_ = mc.proposer.ProposeProposal(&raftcmdpb.Proposal{
		MetadataConversionBatches: []*raftcmdpb.MetadataConversionBatch{{
			Ledger:             ledgerName,
			TargetType:         targetType,
			Key:                key,
			ExpectedType:       expectedType,
			Entries:            entries,
			TotalKeys:          totalKeys,
			ConvertedKeysSoFar: convertedKeysSoFar,
		}},
	})
}

// proposeComplete proposes a MetadataConversionCompletion to Raft as a direct Proposal field.
func (mc *MetadataConverter) proposeComplete(
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
) {
	_ = mc.proposer.ProposeProposal(&raftcmdpb.Proposal{
		MetadataConversionsComplete: []*raftcmdpb.MetadataConversionCompletion{{
			Ledger:       ledgerName,
			TargetType:   targetType,
			Key:          key,
			ExpectedType: expectedType,
		}},
	})
}

// convert scans all account metadata for the specified ledger, finds entries
// matching the declared key, converts values that do not match the expected
// type, and proposes batches of converted entries back through Raft.
//
// Uses two streaming Pebble passes (via StreamingIter) instead of loading all
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
//     worker.ErrNotLeader so the retry loop waits and re-checks.
//   - Only the leader scans, converts, and proposes.
func (mc *MetadataConverter) convert(ctx context.Context, req MetadataConvertRequest) error {
	logFields := map[string]any{
		"ledger": req.LedgerName,
		"key":    req.Key,
		"type":   req.Type.String(),
	}

	if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
		mc.logger.WithFields(logFields).Infof("Field no longer converting (completed by leader), done")

		return nil
	}

	if !mc.isLeader() {
		return worker.ErrNotLeader
	}

	// Validate the ledger exists (off the hot path).
	_, err := query.GetLedgerByName(ctx, mc.dataStore, req.LedgerName)
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

	// Select the attribute store and prefix based on target type.
	attr, ledgerPrefix := mc.attrAndPrefixForTarget(req)

	// Open a Pebble read handle for a point-in-time snapshot used by both passes.
	reader, err := mc.dataStore.NewReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = reader.Close() }()

	// Pass 1: count matching keys for progress tracking (O(1) memory).
	var totalMatchingKeys uint64

	countIter, err := attr.NewStreamingIter(reader, ledgerPrefix)
	if err != nil {
		return fmt.Errorf("creating count iterator for ledger %s: %w", req.LedgerName, err)
	}

	for countIter.Next() {
		entry := countIter.Entry()

		metaKeyName, mkErr := extractMetadataKeyName(req.TargetType, entry.CanonicalKey)
		if mkErr != nil {
			continue // skip unparseable keys
		}

		if metaKeyName == req.Key {
			totalMatchingKeys++
		}
	}

	if err := countIter.Close(); err != nil {
		return fmt.Errorf("closing count iterator for ledger %s: %w", req.LedgerName, err)
	}

	if err := countIter.Err(); err != nil {
		return fmt.Errorf("counting metadata keys for ledger %s: %w", req.LedgerName, err)
	}

	mc.logger.WithFields(map[string]any{
		"ledger":            req.LedgerName,
		"key":               req.Key,
		"totalMatchingKeys": totalMatchingKeys,
	}).Infof("Counted matching metadata keys, starting conversion")

	// Pass 2: convert and propose in batches (O(batch_size) memory).
	batch := make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)

	var convertedSoFar uint64

	convertIter, err := attr.NewStreamingIter(reader, ledgerPrefix)
	if err != nil {
		return fmt.Errorf("creating convert iterator for ledger %s: %w", req.LedgerName, err)
	}

	aborted := false

	for convertIter.Next() {
		entry := convertIter.Entry()

		metaKeyName, mkErr := extractMetadataKeyName(req.TargetType, entry.CanonicalKey)
		if mkErr != nil {
			mc.logger.Errorf("Failed to unmarshal metadata key %x: %v", entry.CanonicalKey, mkErr)

			continue // skip unparseable keys
		}

		if metaKeyName != req.Key {
			continue
		}

		// Check if the value already matches the expected type.
		if commonpb.TypeMatches(entry.Value, req.Type) {
			convertedSoFar++

			continue
		}

		// Convert the value to the expected type.
		convertedValue := commonpb.ConvertMetadataValue(entry.Value, req.Type)

		batch = append(batch, &raftcmdpb.ConvertMetadataEntry{
			CanonicalKey:   append([]byte(nil), entry.CanonicalKey...),
			ConvertedValue: convertedValue,
		})

		if len(batch) >= mc.batchSize {
			// Check staleness before proposing each batch.
			if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
				mc.logger.WithFields(logFields).Infof("Field no longer converting mid-batch, aborting")

				aborted = true

				break
			}

			convertedSoFar += uint64(len(batch))
			mc.proposeBatch(req.LedgerName, req.TargetType, req.Key, req.Type, batch, totalMatchingKeys, convertedSoFar)
			batch = make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)
		}
	}

	if err := convertIter.Close(); err != nil {
		return fmt.Errorf("closing convert iterator for ledger %s: %w", req.LedgerName, err)
	}

	if aborted {
		return nil
	}

	if err := convertIter.Err(); err != nil {
		return fmt.Errorf("converting metadata for ledger %s: %w", req.LedgerName, err)
	}

	// Propose any remaining partial batch.
	if len(batch) > 0 {
		if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
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

// attrAndPrefixForTarget returns the attribute store and canonical prefix to
// scan for the given target type.
func (mc *MetadataConverter) attrAndPrefixForTarget(req MetadataConvertRequest) (*attributes.Attribute[*commonpb.MetadataValue], []byte) {
	switch req.TargetType {
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		// LedgerMetadataKey format: [ledger]\x01[key]
		return mc.attrs.LedgerMetadata, []byte(req.LedgerName + "\x01")
	default:
		// Account MetadataKey format: [ledger]\x00[account]\x01[key]
		return mc.attrs.Metadata, []byte(req.LedgerName + "\x00")
	}
}

// extractMetadataKeyName unmarshals a canonical key and returns the metadata
// key name portion, dispatching based on target type.
func extractMetadataKeyName(targetType commonpb.TargetType, canonicalKey []byte) (string, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		var lmk domain.LedgerMetadataKey
		if err := lmk.Unmarshal(canonicalKey); err != nil {
			return "", err
		}

		return lmk.Key, nil
	default:
		var mk domain.MetadataKey
		if err := mk.Unmarshal(canonicalKey); err != nil {
			return "", err
		}

		return mk.Key, nil
	}
}
