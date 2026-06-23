package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// RebuildDelta reconstructs derived state (attributes, system state) from logs
// that were added by export segments beyond the checkpoint.
// If fromLogSeq is 0, it rebuilds from the very first log (full rebuild).
func RebuildDelta(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	fromLogSeq uint64,
	fromAuditSeq uint64,
) error {
	attrs := attributes.New()
	batch := store.OpenWriteSession()

	writer := &attributeReplayWriter{
		store:          store,
		batch:          batch,
		volume:         attrs.Volume,
		metadata:       attrs.Metadata,
		tx:             attrs.Transaction,
		pendingVolumes: make(map[string]*raftcmdpb.VolumePair),
	}

	sinkConfig := attrs.SinkConfig
	numscriptContent := attrs.NumscriptContent
	numscriptVersion := attrs.NumscriptVersion

	rawLedgerTypes := make(map[string]map[string]*commonpb.AccountType)
	ledgerAccountTypes := make(map[string][]accounttype.CompiledType)

	readHandle, err := store.NewDirectReadHandle()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = readHandle.Close() }()

	// Seed ledger account types from state already in the store. On an
	// incremental rebuild the AddAccountType logs precede fromLogSeq, so
	// without this the replayed entries would skip ephemeral-purge simulation
	// and write transient volumes that should never have been persisted.
	if err := seedLedgerContext(ctx, readHandle, rawLedgerTypes, ledgerAccountTypes); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("seeding ledger context: %w", err)
	}

	logCursor, err := query.ReadLogsSince(ctx, readHandle, fromLogSeq)
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("reading logs since %d: %w", fromLogSeq, err)
	}

	defer func() { _ = logCursor.Close() }()

	proposalBoundaries, err := newProposalBoundaryReader(ctx, readHandle, fromLogSeq, fromAuditSeq)
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("reading proposal log boundaries: %w", err)
	}
	defer func() { _ = proposalBoundaries.Close() }()

	nextProposalEnd, hasProposalEnd, err := proposalBoundaries.Next()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("reading first proposal log boundary: %w", err)
	}

	var ephemeralPurgeBuffer *replay.EphemeralPurgeBuffer
	if hasProposalEnd {
		ephemeralPurgeBuffer = replay.NewEphemeralPurgeBuffer()
	}

	var count uint64

	for {
		if err := ctx.Err(); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("rebuild cancelled after %d logs: %w", count, err)
		}

		log, err := logCursor.Next()
		if errors.Is(err, io.EOF) {
			break // clean end of stream
		}
		if err != nil {
			// A non-EOF error means the log stream was truncated or a record
			// failed to decode. Committing here would report a partial rebuild
			// as success, leaving inconsistent derived state after a restore.
			_ = batch.Cancel()

			return fmt.Errorf("reading log cursor after %d logs: %w", count, err)
		}

		payload := log.GetPayload()
		seq := log.GetSequence()

		for ephemeralPurgeBuffer != nil && hasProposalEnd && seq > nextProposalEnd {
			if err := ephemeralPurgeBuffer.Flush(writer, ledgerAccountTypes, nil); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("flushing replay ephemeral purge at missing log boundary %d: %w", nextProposalEnd, err)
			}

			nextProposalEnd, hasProposalEnd, err = proposalBoundaries.Next()
			if err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("reading next proposal log boundary: %w", err)
			}
		}

		switch p := payload.GetType().(type) {
		case *commonpb.LogPayload_Apply:
			if p.Apply == nil || p.Apply.GetLog() == nil || p.Apply.GetLog().GetData() == nil {
				continue
			}

			ledgerName := p.Apply.GetLedgerName()

			if err := replay.ReplayLedgerLog(ledgerName, seq, p.Apply.GetLog().GetData(), writer, rawLedgerTypes, ledgerAccountTypes, ephemeralPurgeBuffer); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("replaying ledger log %d: %w", seq, err)
			}

		case *commonpb.LogPayload_CreateLedger:
			if p.CreateLedger == nil {
				continue
			}

			info := &commonpb.LedgerInfo{
				Name:      p.CreateLedger.GetName(),
				Id:        p.CreateLedger.GetId(),
				CreatedAt: p.CreateLedger.GetCreatedAt(),
				Mode:      p.CreateLedger.GetMode(),
			}

			if err := state.SaveLedger(batch, info); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("saving ledger info at log %d: %w", seq, err)
			}

		case *commonpb.LogPayload_DeleteLedger:
			// Deletion is handled by system state; nothing to rebuild here

		case *commonpb.LogPayload_PromoteLedger:
			// Promotion changes ledger mode — would need to read and update LedgerInfo.
			// For now, the original CreateLedger captures the initial state.

		case *commonpb.LogPayload_RegisterSigningKey:
			if p.RegisterSigningKey != nil {
				if err := state.SaveSigningKey(batch,
					p.RegisterSigningKey.GetKeyId(),
					p.RegisterSigningKey.GetPublicKey(),
					p.RegisterSigningKey.GetParentKeyId(),
				); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving signing key at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_SetSigningConfig:
			if p.SetSigningConfig != nil {
				if err := state.SaveSigningConfig(batch, p.SetSigningConfig.GetRequireSignatures()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving signing config at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_SetMaintenanceMode:
			if p.SetMaintenanceMode != nil {
				if err := state.SaveMaintenanceMode(batch, p.SetMaintenanceMode.GetEnabled()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving maintenance mode at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_AddedEventsSink:
			if p.AddedEventsSink != nil && p.AddedEventsSink.GetConfig() != nil {
				cfg := p.AddedEventsSink.GetConfig()
				if _, err := sinkConfig.Set(batch, domain.SinkConfigKey{Name: cfg.GetName()}.Bytes(), cfg); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving events sink at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_SetChapterSchedule:
			if p.SetChapterSchedule != nil {
				if err := state.SaveChapterSchedule(batch, p.SetChapterSchedule.GetCron()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving chapter schedule at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_SavedNumscript:
			if p.SavedNumscript != nil && p.SavedNumscript.GetInfo() != nil {
				info := p.SavedNumscript.GetInfo()
				nsLedger := info.GetLedger()
				entryKey := domain.NumscriptEntryKey{LedgerName: nsLedger, Name: info.GetName(), Version: info.GetVersion()}
				if _, err := numscriptContent.Set(batch, entryKey.Bytes(), info); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving numscript at log %d: %w", seq, err)
				}

				versionKey := domain.NumscriptVersionKey{LedgerName: nsLedger, Name: info.GetName()}
				versionVal := &commonpb.NumscriptVersionValue{Version: info.GetVersion()}
				if _, err := numscriptVersion.Set(batch, versionKey.Bytes(), versionVal); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving numscript version at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_CreatedPreparedQuery:
			if p.CreatedPreparedQuery != nil && p.CreatedPreparedQuery.GetQuery() != nil {
				if err := state.SavePreparedQuery(batch, p.CreatedPreparedQuery.GetLedger(), p.CreatedPreparedQuery.GetQuery()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving prepared query at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_UpdatedPreparedQuery:
			// Updated queries contain previous_filter and new_filter, not a full PreparedQuery.
			// The query state is not critical for restore — it can be re-created.

		case *commonpb.LogPayload_SetQueryCheckpointSchedule:
			if p.SetQueryCheckpointSchedule != nil {
				if err := state.SaveQueryCheckpointSchedule(batch, p.SetQueryCheckpointSchedule.GetCron()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving query checkpoint schedule at log %d: %w", seq, err)
				}
			}

		// Log types with no persistent state to rebuild:
		case *commonpb.LogPayload_RevokeSigningKey:
		case *commonpb.LogPayload_RemovedEventsSink:
		case *commonpb.LogPayload_CloseChapter:
		case *commonpb.LogPayload_SealChapter:
		case *commonpb.LogPayload_ArchiveChapter:
		case *commonpb.LogPayload_ConfirmArchiveChapter:
		case *commonpb.LogPayload_DeleteChapterSchedule:
		case *commonpb.LogPayload_DeletedPreparedQuery:
		case *commonpb.LogPayload_DeletedNumscript:
		case *commonpb.LogPayload_CreatedQueryCheckpoint:
		case *commonpb.LogPayload_DeletedQueryCheckpoint:
		case *commonpb.LogPayload_DeleteQueryCheckpointSchedule:
		}

		if ephemeralPurgeBuffer != nil && hasProposalEnd && seq == nextProposalEnd {
			if err := ephemeralPurgeBuffer.Flush(writer, ledgerAccountTypes, nil); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("flushing replay ephemeral purge at log %d: %w", seq, err)
			}

			nextProposalEnd, hasProposalEnd, err = proposalBoundaries.Next()
			if err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("reading next proposal log boundary: %w", err)
			}
		}

		count++

		// Commit in batches to avoid unbounded memory
		if count%5000 == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("committing batch at log %d: %w", seq, err)
			}

			batch = store.OpenWriteSession()
			writer.batch = batch
			clear(writer.pendingVolumes)

			logger.WithFields(map[string]any{
				"logsProcessed": count,
				"currentSeq":    seq,
			}).Infof("RebuildDelta progress")
		}
	}

	if ephemeralPurgeBuffer != nil {
		if err := ephemeralPurgeBuffer.Flush(writer, ledgerAccountTypes, nil); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("flushing final replay ephemeral purge: %w", err)
		}
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing final batch: %w", err)
	}

	logger.WithFields(map[string]any{
		"totalLogsProcessed": count,
	}).Infof("RebuildDelta completed")

	return nil
}

type proposalBoundaryReader struct {
	auditCursor cursor.Cursor[*auditpb.AuditEntry]
	tracker     *replay.ProposalBoundaryTracker
}

func newProposalBoundaryReader(
	ctx context.Context,
	reader dal.PebbleReader,
	replayedThrough uint64,
	afterAuditSeq uint64,
) (*proposalBoundaryReader, error) {
	var after *uint64
	if afterAuditSeq > 0 {
		after = &afterAuditSeq
	}

	auditCursor, err := query.ReadAuditEntries(ctx, reader, after)
	if err != nil {
		return nil, fmt.Errorf("reading audit entries: %w", err)
	}

	return &proposalBoundaryReader{
		auditCursor: auditCursor,
		tracker:     replay.NewProposalBoundaryTracker(replayedThrough),
	}, nil
}

func (r *proposalBoundaryReader) Next() (uint64, bool, error) {
	for {
		entry, err := r.auditCursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, false, nil
			}

			return 0, false, fmt.Errorf("reading audit entry: %w", err)
		}

		success := entry.GetSuccess()
		if success == nil || success.GetMaxLogSequence() == 0 {
			continue
		}

		if boundary, ok := r.tracker.Accept(success.GetMaxLogSequence()); ok {
			return boundary, true, nil
		}
	}
}

func (r *proposalBoundaryReader) Close() error {
	if r == nil || r.auditCursor == nil {
		return nil
	}

	return r.auditCursor.Close()
}

// seedLedgerContext populates the account-type maps from ledgers already
// persisted in the store (i.e. captured by the checkpoint), so an incremental
// replay resolves account-type persistence for ledgers created before
// fromLogSeq.
func seedLedgerContext(
	ctx context.Context,
	reader dal.PebbleReader,
	rawLedgerTypes map[string]map[string]*commonpb.AccountType,
	ledgerAccountTypes map[string][]accounttype.CompiledType,
) error {
	cursor, err := query.ReadLedgers(ctx, reader)
	if err != nil {
		return fmt.Errorf("reading ledgers: %w", err)
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("iterating ledgers: %w", err)
		}

		name := info.GetName()

		if types := info.GetAccountTypes(); len(types) > 0 {
			cloned := maps.Clone(types)
			rawLedgerTypes[name] = cloned
			ledgerAccountTypes[name] = accounttype.CompileTypes(cloned)
		}
	}

	return nil
}

// attributeReplayWriter implements replay.Writer by writing directly to
// Pebble attributes via Attribute[V].Set/Get/Delete.
type attributeReplayWriter struct {
	store          *dal.Store
	batch          *dal.WriteSession
	volume         *attributes.Attribute[*raftcmdpb.VolumePair]
	metadata       *attributes.Attribute[*commonpb.MetadataValue]
	tx             *attributes.Attribute[*commonpb.TransactionState]
	pendingVolumes map[string]*raftcmdpb.VolumePair
}

func (w *attributeReplayWriter) AddVolumeDelta(canonicalKey []byte, inputDelta, outputDelta *big.Int) error {
	existing, err := w.GetVolume(canonicalKey)
	if err != nil {
		return err
	}

	var inVal, outVal uint256.Int
	if existing != nil {
		if existing.GetInput() != nil {
			inVal.SetFromBig(existing.GetInput().ToBigInt())
		}

		if existing.GetOutput() != nil {
			outVal.SetFromBig(existing.GetOutput().ToBigInt())
		}
	}

	var deltaIn, deltaOut uint256.Int

	deltaIn.SetFromBig(inputDelta)
	deltaOut.SetFromBig(outputDelta)

	inVal.Add(&inVal, &deltaIn)
	outVal.Add(&outVal, &deltaOut)

	pair := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(&inVal),
		Output: commonpb.NewUint256(&outVal),
	}

	_, err = w.volume.Set(w.batch, canonicalKey, pair)
	if err == nil {
		w.pendingVolumes[string(canonicalKey)] = pair
	}

	return err
}

func (w *attributeReplayWriter) GetVolume(canonicalKey []byte) (*raftcmdpb.VolumePair, error) {
	if pair, ok := w.pendingVolumes[string(canonicalKey)]; ok {
		return pair, nil
	}

	return w.volume.Get(w.store, canonicalKey)
}

func (w *attributeReplayWriter) DeleteVolume(canonicalKey []byte) error {
	err := w.volume.Delete(w.batch, canonicalKey)
	if err == nil {
		w.pendingVolumes[string(canonicalKey)] = nil
	}

	return err
}

func (w *attributeReplayWriter) MoveVolume(oldKey, newKey []byte) error {
	oldVol, err := w.GetVolume(oldKey)
	if err != nil {
		return err
	}

	if oldVol == nil {
		return nil
	}

	inBig := oldVol.GetInput().ToBigInt()
	outBig := oldVol.GetOutput().ToBigInt()

	if err := w.AddVolumeDelta(newKey, inBig, outBig); err != nil {
		return err
	}

	return w.DeleteVolume(oldKey)
}

func (w *attributeReplayWriter) SetMetadata(canonicalKey []byte, value string) error {
	mv := &commonpb.MetadataValue{}
	mv.Type = &commonpb.MetadataValue_StringValue{StringValue: value}

	_, err := w.metadata.Set(w.batch, canonicalKey, mv)

	return err
}

func (w *attributeReplayWriter) DeleteMetadata(canonicalKey []byte) error {
	return w.metadata.Delete(w.batch, canonicalKey)
}

func (w *attributeReplayWriter) MoveMetadata(oldKey, newKey []byte) error {
	oldVal, err := w.metadata.Get(w.store, oldKey)
	if err != nil {
		return err
	}

	if oldVal == nil {
		return nil
	}

	if _, err := w.metadata.Set(w.batch, newKey, oldVal); err != nil {
		return err
	}

	return w.metadata.Delete(w.batch, oldKey)
}

func (w *attributeReplayWriter) CreateTransaction(canonicalKey []byte, seq uint64, timestamp *commonpb.Timestamp, metadata map[string]*commonpb.MetadataValue) error {
	txState := &commonpb.TransactionState{
		CreatedByLog: seq,
		Metadata:     metadata,
		Timestamp:    timestamp,
	}

	_, err := w.tx.Set(w.batch, canonicalKey, txState)

	return err
}

func (w *attributeReplayWriter) SetRevertedBy(canonicalKey []byte, revertTxID uint64) error {
	existing, err := w.tx.Get(w.store, canonicalKey)
	if err != nil {
		return err
	}

	if existing == nil {
		existing = &commonpb.TransactionState{}
	}

	existing.RevertedByTransaction = revertTxID

	_, err = w.tx.Set(w.batch, canonicalKey, existing)

	return err
}

func (w *attributeReplayWriter) SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error {
	existing, err := w.tx.Get(w.store, canonicalKey)
	if err != nil {
		return err
	}

	if existing == nil {
		existing = &commonpb.TransactionState{}
	}

	if existing.GetMetadata() == nil {
		existing.Metadata = make(map[string]*commonpb.MetadataValue)
	}

	maps.Copy(existing.GetMetadata(), metadata)

	_, err = w.tx.Set(w.batch, canonicalKey, existing)

	return err
}

func (w *attributeReplayWriter) DeleteTxMetadata(canonicalKey []byte, key string) error {
	existing, err := w.tx.Get(w.store, canonicalKey)
	if err != nil {
		return err
	}

	if existing == nil || existing.GetMetadata() == nil {
		return nil
	}

	delete(existing.GetMetadata(), key)

	_, err = w.tx.Set(w.batch, canonicalKey, existing)

	return err
}
