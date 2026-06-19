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

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source=metadata_converter.go -destination=metadata_converter_generated_test.go -package=state -mock_names=MetadataBatchProposer=MockMetadataBatchProposer

// MetadataBatchProposer abstracts the preload + propose path for the
// metadata converter. The implementation (in bootstrap) constructs a
// plan.Needs from the canonical keys, runs the proposal through
// plan.Builder.RunWithPreload, and waits for the FSM apply.
//
// The interface is defined here rather than importing preload because
// preload depends on state (for ApplyResult), which would create an
// import cycle.
type MetadataBatchProposer interface {
	Propose(ctx context.Context, cmd *raftcmdpb.Proposal, canonicalKeys [][]byte, target commonpb.TargetType) error
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
	dataStore   dal.BackgroundReader
	attrs       *attributes.Attributes
	requestCh   *worker.Channel[MetadataConvertRequest]
	proposer    MetadataBatchProposer
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
	dataStore dal.BackgroundReader,
	attrs *attributes.Attributes,
	requestCh *worker.Channel[MetadataConvertRequest],
	proposer MetadataBatchProposer,
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

	mc.w.RunCtx(mc.dispatchLoop)
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
//
// ctx is supplied by Worker.RunCtx and is cancelled by Stop(). It flows to
// each convertWithRetry call so an in-flight conversion unblocks on
// shutdown, and to worker.RetryWithBackoff via ctx.Done() at the API
// boundary.
func (mc *MetadataConverter) dispatchLoop(ctx context.Context) {
	sem := make(chan struct{}, mc.poolSize)

	var pending []MetadataConvertRequest

	for {
		// When we have pending work, try to dispatch it alongside accepting
		// new requests and checking for stop.
		if len(pending) > 0 {
			select {
			case <-ctx.Done():
				return
			case req := <-mc.requestCh.Receive():
				pending = append(pending, req)
			case sem <- struct{}{}:
				req := pending[0]
				pending = pending[1:]

				mc.wg.Go(func() {
					defer func() { <-sem }()

					mc.convertWithRetry(ctx, req)
				})
			}
		} else {
			// Nothing pending: just wait for new work or stop.
			select {
			case <-ctx.Done():
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
func (mc *MetadataConverter) convertWithRetry(ctx context.Context, req MetadataConvertRequest) {
	// ctx.Done() bridges to RetryWithBackoff, which still consumes a
	// stop <-chan struct{} signal — same shape, same semantics.
	worker.RetryWithBackoff(ctx.Done(), mc.logger, func() error {
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

// proposeBatch submits a MetadataConversionBatch through the
// MetadataBatchProposer. The implementation runs the proposal through
// plan.Builder.RunWithPreload so each canonical key is in the
// FSM cache before applyMetadataConversionBatch runs its per-entry
// compare-and-set — eliminating the cache-miss path that previously
// caused conversions to be silently skipped while progress was still
// recorded as complete (PR #359 regression of #313).
func (mc *MetadataConverter) proposeBatch(
	ctx context.Context,
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
	entries []*raftcmdpb.ConvertMetadataEntry,
) error {
	cmd := &raftcmdpb.Proposal{
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MetadataBatch{
				MetadataBatch: &raftcmdpb.MetadataConversionBatch{
					Ledger:       ledgerName,
					TargetType:   targetType,
					Key:          key,
					ExpectedType: expectedType,
					Entries:      entries,
				},
			},
		}},
	}

	canonicalKeys := make([][]byte, len(entries))
	for i, e := range entries {
		canonicalKeys[i] = e.GetCanonicalKey()
	}

	return mc.proposer.Propose(ctx, cmd, canonicalKeys, targetType)
}

// proposeComplete submits a MetadataConversionCompletion. Used by the
// TRANSACTION target type (read-time enforcement, no scan) and by the
// ACCOUNT/LEDGER path once a full Pebble scan turns up zero entries
// needing conversion — the only signal the FSM accepts to flip a
// field to COMPLETE.
//
// Freshness of the LedgerInfo at apply time is delegated to the standard
// preload path (see metadataBatchProposer.Propose): the proposer adds the
// ledger key to `needs.Ledgers`, the runner resolves it fresh at propose
// time (Pebble read on cache miss) and `PredictedIndex` catches any
// mutation between propose and apply.
func (mc *MetadataConverter) proposeComplete(
	ctx context.Context,
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	expectedType commonpb.MetadataType,
) error {
	cmd := &raftcmdpb.Proposal{
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MetadataCompletion{
				MetadataCompletion: &raftcmdpb.MetadataConversionCompletion{
					Ledger:       ledgerName,
					TargetType:   targetType,
					Key:          key,
					ExpectedType: expectedType,
				},
			},
		}},
	}

	return mc.proposer.Propose(ctx, cmd, nil, targetType)
}

// maxConvertPasses bounds how many back-to-back scan passes convert() will
// run before yielding back to the reconcile loop. This is a livelock backstop
// against a workload that keeps writing mismatched-type values faster than the
// converter can catch up; without it convert() could loop forever for a
// single MetadataConvertRequest. The next reconcile tick picks the field up
// again if convergence hasn't been reached.
const maxConvertPasses = 64

// convert scans all metadata for the specified ledger, finds entries matching
// the declared key, converts values that do not match the expected type, and
// proposes batches of converted entries back through Raft.
//
// A streaming Pebble pass (via StreamingIter) iterates without loading all
// entries into memory. Multiple passes are run back-to-back from the same
// convert() call: each batch waits on FSM apply before the next batch is
// proposed, so when one pass ends every preceding write is already in the
// cache and the next pass sees it. The loop terminates as soon as a pass
// enqueues zero entries — meaning every matching value already has the
// expected type — at which point convert() proposes
// MetadataConversionCompletion. Bounded by maxConvertPasses against
// pathological write-heavy workloads; the reconcile loop is the fallback.
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

	// Resolve the ledger so the caller can fail fast if it has been deleted
	// while a conversion request was in flight. The canonical-key prefix is
	// derived directly from the ledger name (padded fixed-width) — matching
	// how the live write paths build keys in `domain.MetadataKey` /
	// `domain.LedgerMetadataKey` since the LedgerID → LedgerName refactor.
	if _, err := query.GetLedgerByName(ctx, mc.dataStore, req.LedgerName); err != nil {
		return fmt.Errorf("resolving ledger %q: %w", req.LedgerName, err)
	}

	// Transaction metadata uses read-time enforcement (assembleTransaction replays
	// append-only update logs on every read). No background scan needed — just
	// propose completion to transition CONVERTING → COMPLETE.
	if req.TargetType == commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		if err := mc.proposeComplete(ctx, req.LedgerName, req.TargetType, req.Key, req.Type); err != nil {
			return fmt.Errorf("proposing transaction metadata conversion completion: %w", err)
		}

		mc.logger.WithFields(logFields).Infof("Transaction metadata conversion complete (read-time enforcement)")

		return nil
	}

	mc.logger.WithFields(logFields).Infof("Starting metadata conversion")

	for pass := range maxConvertPasses {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
			mc.logger.WithFields(logFields).Infof("Field no longer converting (completed by Raft), done")

			return nil
		}

		entriesEnqueued, aborted, err := mc.runConvertPass(ctx, req, logFields)
		if err != nil {
			return err
		}

		if aborted {
			return nil
		}

		// Convergence: a pass that enqueued zero entries means every
		// matching value already has the expected type. Propose
		// Complete — the only signal the FSM accepts to flip the
		// ACCOUNT/LEDGER field to COMPLETE. LedgerInfo freshness is
		// the preload runner's job (see metadataBatchProposer.Propose),
		// so no snapshot is captured here.
		if entriesEnqueued == 0 {
			if err := mc.proposeComplete(ctx, req.LedgerName, req.TargetType, req.Key, req.Type); err != nil {
				return fmt.Errorf("proposing metadata conversion completion: %w", err)
			}

			mc.logger.WithFields(map[string]any{
				"ledger": req.LedgerName,
				"key":    req.Key,
				"passes": pass + 1,
			}).Infof("Conversion converged; proposed COMPLETE")

			return nil
		}

		mc.logger.WithFields(map[string]any{
			"ledger":          req.LedgerName,
			"key":             req.Key,
			"pass":            pass + 1,
			"entriesEnqueued": entriesEnqueued,
		}).Debugf("Metadata conversion pass enqueued entries; running another pass")
	}

	// Pass cap reached without convergence — yield to the reconcile loop.
	// In practice this only fires under a workload that mutates faster
	// than the converter applies; the reconcile tick picks it up again.
	mc.logger.WithFields(map[string]any{
		"ledger":    req.LedgerName,
		"key":       req.Key,
		"maxPasses": maxConvertPasses,
	}).Errorf("Metadata conversion did not converge within max passes; yielding to reconcile loop")

	return nil
}

// runConvertPass executes one streaming scan of the metadata zone for the
// ledger+target and proposes batches of conversions. Returns the count of
// entries it enqueued (i.e. needing a write) and an aborted flag indicating
// the field is no longer CONVERTING (caller must stop, not retry).
func (mc *MetadataConverter) runConvertPass(
	ctx context.Context,
	req MetadataConvertRequest,
	logFields map[string]any,
) (entriesEnqueued uint64, aborted bool, err error) {
	attr, ledgerPrefix := mc.attrAndPrefixForTarget(req.TargetType, req.LedgerName)

	reader, err := mc.dataStore.NewReadHandle()
	if err != nil {
		return 0, false, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = reader.Close() }()

	batch := make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)

	iter, err := attr.NewStreamingIter(reader, ledgerPrefix)
	if err != nil {
		return 0, false, fmt.Errorf("creating iterator for ledger %s: %w", req.LedgerName, err)
	}

	for iter.Next() {
		entry := iter.Entry()

		metaKeyName, mkErr := extractMetadataKeyName(req.TargetType, entry.CanonicalKey)
		if mkErr != nil {
			mc.logger.Errorf("Failed to unmarshal metadata key %x: %v", entry.CanonicalKey, mkErr)

			continue // skip unparseable keys
		}

		if metaKeyName != req.Key {
			continue
		}

		// Values that already match the expected type don't need a
		// write. They are how the pass eventually finds zero entries
		// to enqueue and proposes Complete.
		if commonpb.TypeMatches(entry.Value, req.Type) {
			continue
		}

		convertedValue := commonpb.ConvertMetadataValue(entry.Value, req.Type)

		// If the conversion produced no change relative to what's
		// already on disk, the key has converged for this target — skip
		// it. The case we care about is NullValue → NullValue with the
		// same Original: `ConvertMetadataValue` writes a NullValue
		// sentinel whenever the value is structurally inconvertible
		// (e.g. "abc" → INT64), and re-enqueuing on every pass would
		// loop forever. NB this is NOT a blanket "skip NullValue":
		// retyping a field can make a previously-failed value
		// convertible (e.g. STRING → INT64 → STRING again recovers the
		// original string), so we let ConvertMetadataValue decide — if
		// it returns a different NullValue (different Original) or any
		// non-NullValue, the entry is enqueued.
		if isUnchangedNullValue(entry.Value, convertedValue) {
			continue
		}

		// Snapshot the value we just scanned. The FSM apply path uses
		// it as a compare-and-set guard: if the preload-resolved cache
		// value at apply time differs from this snapshot (user mutation
		// or deletion landed in Raft order between scan and apply), the
		// conversion is skipped so we don't resurrect a deleted value
		// or clobber a fresh write. See #313 (cache-evicted, now solved
		// by preload) and #359 (mutation race).
		expectedBytes, marshalErr := entry.Value.MarshalVT()
		if marshalErr != nil {
			_ = iter.Close()

			return entriesEnqueued, false, fmt.Errorf("marshaling expected value for %s: %w", req.LedgerName, marshalErr)
		}

		batch = append(batch, &raftcmdpb.ConvertMetadataEntry{
			CanonicalKey:   append([]byte(nil), entry.CanonicalKey...),
			ConvertedValue: convertedValue,
			ExpectedValue:  expectedBytes,
		})
		entriesEnqueued++

		if len(batch) >= mc.batchSize {
			if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
				mc.logger.WithFields(logFields).Infof("Field no longer converting mid-batch, aborting")

				_ = iter.Close()

				return entriesEnqueued, true, nil
			}

			if proposeErr := mc.proposeBatch(ctx, req.LedgerName, req.TargetType, req.Key, req.Type, batch); proposeErr != nil {
				_ = iter.Close()

				return entriesEnqueued, false, fmt.Errorf("proposing metadata conversion batch: %w", proposeErr)
			}

			batch = make([]*raftcmdpb.ConvertMetadataEntry, 0, mc.batchSize)
		}
	}

	if closeErr := iter.Close(); closeErr != nil {
		return entriesEnqueued, false, fmt.Errorf("closing iterator for ledger %s: %w", req.LedgerName, closeErr)
	}

	if iterErr := iter.Err(); iterErr != nil {
		return entriesEnqueued, false, fmt.Errorf("converting metadata for ledger %s: %w", req.LedgerName, iterErr)
	}

	// Propose any remaining partial batch.
	if len(batch) > 0 {
		if !mc.isFieldStillConverting(ctx, req.LedgerName, req.TargetType, req.Key, req.Type) {
			mc.logger.WithFields(logFields).Infof("Field no longer converting mid-batch, aborting")

			return entriesEnqueued, true, nil
		}

		if proposeErr := mc.proposeBatch(ctx, req.LedgerName, req.TargetType, req.Key, req.Type, batch); proposeErr != nil {
			return entriesEnqueued, false, fmt.Errorf("proposing metadata conversion batch: %w", proposeErr)
		}
	}

	return entriesEnqueued, false, nil
}

// attrAndPrefixForTarget returns the attribute store and canonical prefix to
// scan for the given target type. The prefix is the ledger name padded to
// LedgerNameFixedSize — matching how the live write paths build canonical
// keys in `domain.MetadataKey` and `domain.LedgerMetadataKey` since the
// LedgerID → LedgerName refactor.
func (mc *MetadataConverter) attrAndPrefixForTarget(targetType commonpb.TargetType, ledgerName string) (*attributes.Attribute[*commonpb.MetadataValue], []byte) {
	prefix := make([]byte, dal.LedgerNameFixedSize)
	copy(prefix, ledgerName)

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		// LedgerMetadataKey format: [ledgerName padded 64B]\x01[key]
		return mc.attrs.LedgerMetadata, append(prefix, 0x01)
	default:
		// MetadataKey format: [ledgerName padded 64B][account]\x01[key]
		return mc.attrs.Metadata, prefix
	}
}

// isUnchangedNullValue reports whether `current` and `converted` are both
// NullValue sentinels carrying the same Original string. That's the case
// `ConvertMetadataValue` produces when the stored value is structurally
// inconvertible to the target type (e.g. "abc" → INT64): the converter
// would keep re-writing the same NullValue forever and never reach COMPLETE.
//
// This is intentionally narrower than "is NullValue": a retype that makes
// the original convertible again (e.g. INT64 → STRING recovers the
// original string from the NullValue's `Original`) must NOT be skipped.
// In that case `converted` is a non-NullValue (or a NullValue with a
// different Original) and the entry is enqueued normally.
func isUnchangedNullValue(current, converted *commonpb.MetadataValue) bool {
	curNull, curOk := current.GetType().(*commonpb.MetadataValue_NullValue)
	if !curOk {
		return false
	}

	convNull, convOk := converted.GetType().(*commonpb.MetadataValue_NullValue)
	if !convOk {
		return false
	}

	return curNull.NullValue.GetOriginal() == convNull.NullValue.GetOriginal()
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
