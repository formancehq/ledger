package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"

	"github.com/cockroachdb/pebble/v2"
	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
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
		store:           store,
		batch:           batch,
		volume:          attrs.Volume,
		metadata:        attrs.Metadata,
		tx:              attrs.Transaction,
		ledger:          attrs.Ledger,
		references:      attrs.References,
		boundary:        attrs.Boundary,
		pendingVolumes:  make(map[string]*raftcmdpb.VolumePair),
		pendingTx:       make(map[string]*commonpb.TransactionState),
		ledgerInfos:     make(map[string]*commonpb.LedgerInfo),
		boundaries:      make(map[string]*raftcmdpb.LedgerBoundaries),
		reversions:      make(map[string]*bitset.Bitset),
		dirtyReversions: make(map[string]struct{}),
	}

	sinkConfig := attrs.SinkConfig
	numscriptContent := attrs.NumscriptContent
	numscriptVersion := attrs.NumscriptVersion
	ledgerMetadata := attrs.LedgerMetadata

	rawLedgerTypes := make(map[string]map[string]*commonpb.AccountType)
	ledgerAccountTypes := make(map[string][]accounttype.CompiledType)

	readHandle, err := store.NewDirectReadHandle()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = readHandle.Close() }()

	writer.readHandle = readHandle

	// Seed ledger account types from state already in the store. On an
	// incremental rebuild the AddAccountType logs precede fromLogSeq, so
	// without this the replayed entries would skip ephemeral-purge simulation
	// and write transient volumes that should never have been persisted.
	if err := seedLedgerContext(ctx, readHandle, rawLedgerTypes, ledgerAccountTypes, writer.ledgerInfos); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("seeding ledger context: %w", err)
	}

	// Seed the reversion bitsets from the checkpoint so delta reverts fold
	// into (not clobber) the words that already hold pre-checkpoint reverts.
	seededReversions, err := query.ReadReversions(readHandle)
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("seeding reversion bitsets: %w", err)
	}

	writer.reversions = seededReversions

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

			if err := writer.advanceLogID(ledgerName, p.Apply.GetLog().GetId()); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("advancing boundary log id at log %d: %w", seq, err)
			}

		case *commonpb.LogPayload_CreateLedger:
			if p.CreateLedger == nil {
				continue
			}

			// Reconstruct the full LedgerInfo from the creation log — including
			// MirrorSource, AccountTypes, and DefaultEnforcementMode, all part of
			// the stored projection. ToLedgerInfo copies every creation-time field.
			info := p.CreateLedger.ToLedgerInfo()

			if err := writer.saveLedgerInfo(info); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("saving ledger info at log %d: %w", seq, err)
			}

			// Seed the account-type maps from the creation log so replay of this
			// ledger's later logs resolves enforcement / purge simulation against
			// its creation-time chart (mirrors seedLedgerContext for checkpoint
			// ledgers).
			if types := p.CreateLedger.GetAccountTypes(); len(types) > 0 {
				cloned := maps.Clone(types)
				rawLedgerTypes[info.GetName()] = cloned
				ledgerAccountTypes[info.GetName()] = accounttype.CompileTypes(cloned)
			}

			writer.initBoundaries(info.GetName())

		case *commonpb.LogPayload_DeleteLedger:
			if p.DeleteLedger == nil {
				continue
			}

			if err := writer.deleteLedger(p.DeleteLedger.GetName(), p.DeleteLedger.GetDeletedAt(), seq); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("replaying ledger deletion at log %d: %w", seq, err)
			}

			// Enforcement / purge context must not outlive the ledger.
			// Recreation is impossible (processCreateLedger rejects
			// tombstoned names), so nothing reseeds these.
			delete(rawLedgerTypes, p.DeleteLedger.GetName())
			delete(ledgerAccountTypes, p.DeleteLedger.GetName())

		case *commonpb.LogPayload_PromoteLedger:
			if p.PromoteLedger == nil {
				continue
			}

			if err := writer.promoteLedger(p.PromoteLedger.GetName()); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("replaying ledger promotion at log %d: %w", seq, err)
			}

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

		case *commonpb.LogPayload_SavedLedgerMetadata:
			if p.SavedLedgerMetadata != nil {
				for key, value := range p.SavedLedgerMetadata.GetMetadata() {
					mk := domain.LedgerMetadataKey{LedgerName: p.SavedLedgerMetadata.GetLedger(), Key: key}
					if _, err := ledgerMetadata.Set(batch, mk.Bytes(), value); err != nil {
						_ = batch.Cancel()

						return fmt.Errorf("saving ledger metadata at log %d: %w", seq, err)
					}
				}
			}

		case *commonpb.LogPayload_DeletedLedgerMetadata:
			if p.DeletedLedgerMetadata != nil {
				mk := domain.LedgerMetadataKey{LedgerName: p.DeletedLedgerMetadata.GetLedger(), Key: p.DeletedLedgerMetadata.GetKey()}
				if err := ledgerMetadata.Delete(batch, mk.Bytes()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("deleting ledger metadata at log %d: %w", seq, err)
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
			clear(writer.pendingTx)

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

	if err := writer.applyAuditOrderEffects(readHandle, fromLogSeq, fromAuditSeq); err != nil {
		return fmt.Errorf("applying audit order effects to boundaries: %w", err)
	}

	// Net attribute counts (VolumeCount, MetadataCount, ReferenceCount) are
	// read from the committed 0xF1 state, so this runs after the attribute
	// commit. The boundaries themselves are then written in their own batch.
	countHandle, err := store.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("opening read handle for boundary counts: %w", err)
	}
	defer func() { _ = countHandle.Close() }()

	if err := writer.countNetAttributes(countHandle); err != nil {
		return fmt.Errorf("counting net attributes for boundaries: %w", err)
	}

	writer.batch = store.OpenWriteSession()

	if err := writer.flushBoundaries(); err != nil {
		_ = writer.batch.Cancel()

		return fmt.Errorf("flushing rebuilt boundaries: %w", err)
	}

	if err := writer.flushReversions(); err != nil {
		_ = writer.batch.Cancel()

		return fmt.Errorf("flushing rebuilt reversion bitsets: %w", err)
	}

	if err := writer.batch.Commit(); err != nil {
		return fmt.Errorf("committing boundaries batch: %w", err)
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
	ledgerInfos map[string]*commonpb.LedgerInfo,
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

		ledgerInfos[name] = info.CloneVT()

		if types := info.GetAccountTypes(); len(types) > 0 {
			cloned := maps.Clone(types)
			rawLedgerTypes[name] = cloned
			ledgerAccountTypes[name] = accounttype.CompileTypes(cloned)
		}
	}

	return nil
}

// SetMetadataFieldType folds a field-type declaration replayed from the log onto
// the ledger's in-memory LedgerInfo and re-saves it. The schema lives on
// LedgerInfo, which the attribute zones do not cover, so without this a restore
// loses every field type declared beyond the checkpoint.
func (w *attributeReplayWriter) SetMetadataFieldType(ledger string, target commonpb.TargetType, key string, fieldType commonpb.MetadataType) error {
	// Every live ledger is seeded into ledgerInfos from the checkpoint
	// (seedLedgerContext) or from its CreateLedger log during replay, so a schema
	// op with no LedgerInfo means the log stream references a ledger that was
	// never created — a corrupt/impossible stream, not a runtime condition.
	info := w.ledgerInfos[ledger]
	if info == nil {
		return fmt.Errorf("invariant: SetMetadataFieldType for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", ledger)
	}

	if info.GetMetadataSchema() == nil {
		info.MetadataSchema = &commonpb.MetadataSchema{}
	}

	field := &commonpb.MetadataFieldSchema{Type: fieldType}

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		if info.MetadataSchema.AccountFields == nil {
			info.MetadataSchema.AccountFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.AccountFields[key] = field
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		if info.MetadataSchema.TransactionFields == nil {
			info.MetadataSchema.TransactionFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.TransactionFields[key] = field
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		if info.MetadataSchema.LedgerFields == nil {
			info.MetadataSchema.LedgerFields = make(map[string]*commonpb.MetadataFieldSchema)
		}

		info.MetadataSchema.LedgerFields[key] = field
	}

	return w.saveLedgerInfo(info)
}

// RemoveMetadataFieldType drops a field-type declaration from the ledger's
// in-memory LedgerInfo and re-saves it.
func (w *attributeReplayWriter) RemoveMetadataFieldType(ledger string, target commonpb.TargetType, key string) error {
	info := w.ledgerInfos[ledger]
	if info == nil {
		return fmt.Errorf("invariant: RemoveMetadataFieldType for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", ledger)
	}

	// No schema at all means there is nothing to remove — a benign no-op (the
	// field is already absent), unlike a missing ledger.
	if info.GetMetadataSchema() == nil {
		return nil
	}

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		delete(info.GetMetadataSchema().GetAccountFields(), key)
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		delete(info.GetMetadataSchema().GetTransactionFields(), key)
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		delete(info.GetMetadataSchema().GetLedgerFields(), key)
	}

	return w.saveLedgerInfo(info)
}

// saveLedgerInfo persists a LedgerInfo to BOTH the SubAttrLedger attribute
// projection (read by the FSM hot path, the index builder, and preload) and the
// Global LedgerInfo zone (read by the query path and the checker), matching the
// dual write the normal FSM apply performs. Writing only the Global zone would
// leave the attribute projection stale after a restore, so the hot path would see
// checkpoint-era ledger state and the next mutation would clone it over the
// rebuilt fields.
func (w *attributeReplayWriter) saveLedgerInfo(info *commonpb.LedgerInfo) error {
	if _, err := w.ledger.Set(w.batch, domain.LedgerKey{Name: info.GetName()}.Bytes(), info); err != nil {
		return fmt.Errorf("saving rebuilt ledger attribute: %w", err)
	}

	if err := state.SaveLedger(w.batch, info); err != nil {
		return fmt.Errorf("saving rebuilt ledger info: %w", err)
	}

	w.ledgerInfos[info.GetName()] = info

	return nil
}

// AddAccountType folds an account-type declaration replayed from the log onto the
// ledger's in-memory LedgerInfo and re-persists it. Account types live on
// LedgerInfo, so without this a restore loses every type declared beyond the
// checkpoint.
func (w *attributeReplayWriter) AddAccountType(ledger string, accountType *commonpb.AccountType) error {
	info := w.ledgerInfos[ledger]
	if info == nil {
		return fmt.Errorf("invariant: AddAccountType for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", ledger)
	}

	if info.AccountTypes == nil {
		info.AccountTypes = make(map[string]*commonpb.AccountType)
	}

	info.AccountTypes[accountType.GetName()] = accountType

	return w.saveLedgerInfo(info)
}

// RemoveAccountType drops an account-type declaration from the ledger's in-memory
// LedgerInfo and re-persists it.
func (w *attributeReplayWriter) RemoveAccountType(ledger string, name string) error {
	info := w.ledgerInfos[ledger]
	if info == nil {
		return fmt.Errorf("invariant: RemoveAccountType for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", ledger)
	}

	delete(info.GetAccountTypes(), name)

	return w.saveLedgerInfo(info)
}

// attributeReplayWriter implements replay.Writer by writing directly to
// Pebble attributes via Attribute[V].Set/Get/Delete.
//
// Pebble batches are not indexed (see OpenWriteSession), so writes committed
// through w.batch are invisible to w.store.Get until Commit. The pending*
// overlays make in-batch state visible to same-batch reads; both maps are
// cleared on every batch commit alongside the batch itself.
type attributeReplayWriter struct {
	store          *dal.Store
	batch          *dal.WriteSession
	volume         *attributes.Attribute[*raftcmdpb.VolumePair]
	metadata       *attributes.Attribute[*commonpb.MetadataValue]
	tx             *attributes.Attribute[*commonpb.TransactionState]
	ledger         *attributes.Attribute[*commonpb.LedgerInfo]
	references     *attributes.Attribute[*commonpb.TransactionReferenceValue]
	boundary       *attributes.Attribute[*raftcmdpb.LedgerBoundaries]
	pendingVolumes map[string]*raftcmdpb.VolumePair
	pendingTx      map[string]*commonpb.TransactionState

	// LedgerInfo per ledger, carrying the evolving metadata schema and account
	// types. Both live on LedgerInfo (not a per-key attribute), so schema and
	// account-type replays fold into these and re-save. Seeded from the
	// checkpoint and extended as CreateLedger logs replay.
	ledgerInfos map[string]*commonpb.LedgerInfo

	// LedgerBoundaries per touched ledger. The apply path preloads boundaries
	// from the SubAttrBoundary attribute, which the log replay does not write
	// per-entry: NextTransactionId / NextLogId and the per-transaction counters
	// accumulate here, seeded from the checkpoint on first touch via
	// readHandle, and are flushed in their own batch after the attribute
	// commit (net counts are derived from the committed 0xF1 state).
	boundaries map[string]*raftcmdpb.LedgerBoundaries
	readHandle dal.PebbleReader

	// Reversion bitsets per ledger (ZonePerLedger/SubPLReversions). The FSM's
	// already-reverted gate reads these — not the tx rows'
	// RevertedByTransaction markers — so every replayed RevertedTransaction
	// must fold into them or a restored node re-admits reverts of
	// already-reverted transactions. Seeded from the checkpoint rows; ledgers
	// touched by the replay are flushed by flushReversions alongside the
	// boundaries.
	reversions      map[string]*bitset.Bitset
	dirtyReversions map[string]struct{}
}

// applyAuditOrderEffects folds order-level boundary effects that the ledger-log
// stream does not carry: MirrorFillGap's skipped transaction ids (FilledGapLog
// keeps only the original v2 id) and NumscriptExecutionCount (CreatedTransaction
// logs record the resulting postings, not the content source). Both live on the
// order itself, which AuditItem.serialized_order preserves — bound into the
// audit hash chain and shipped by the incremental export's auditItem segments.
//
// Items with log_sequence == 0 (failed proposals, idempotent replays) and items
// at or below fromLogSeq (already folded into the checkpoint) contribute
// nothing. See replay.OrderEffects for why script detection is exact.
func (w *attributeReplayWriter) applyAuditOrderEffects(reader dal.PebbleReader, fromLogSeq, fromAuditSeq uint64) error {
	lower := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
		PutUint64(fromAuditSeq + 1).
		Build()
	upper := []byte{dal.ZoneCold, dal.SubColdAuditItem + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("creating audit item iter: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading audit item value: %w", err)
		}

		item := &auditpb.AuditItem{}
		if err := item.UnmarshalVT(value); err != nil {
			return fmt.Errorf("unmarshaling audit item %x: %w", iter.Key(), err)
		}

		if item.GetLogSequence() == 0 || item.GetLogSequence() <= fromLogSeq {
			continue
		}

		effects, err := replay.DecodeOrderEffects(item.GetSerializedOrder())
		if err != nil {
			return fmt.Errorf("decoding order from audit item %x: %w", iter.Key(), err)
		}

		if effects.Ledger == "" {
			continue
		}

		info := w.ledgerInfos[effects.Ledger]
		if info == nil {
			return fmt.Errorf("invariant: audit item for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", effects.Ledger)
		}

		// A ledger deleted later in the delta has no boundary row in the end
		// state (deleteLedger dropped it); its earlier activity must not
		// resurrect one.
		if info.GetDeletedAt() != nil {
			continue
		}

		b, err := w.boundaryFor(effects.Ledger)
		if err != nil {
			return err
		}

		for _, id := range effects.SkippedTransactionIDs {
			if next := id + 1; next > b.GetNextTransactionId() {
				b.NextTransactionId = next
			}
		}

		if effects.IsNumscript {
			b.NumscriptExecutionCount++
		}
	}

	return iter.Error()
}

// deleteLedger reproduces the live DeleteLedger apply state: the LedgerInfo
// tombstone (DeletedAt), the boundary-row drop, and the pending-cleanup marker.
// The ledger's data rows stay in place — on the live path they are purged only
// when a covering purge range (chapter archival) executes the deferred cleanup,
// which the restored cluster picks up through the same marker
// (ReadPendingLedgerCleanups at boot).
func (w *attributeReplayWriter) deleteLedger(name string, deletedAt *commonpb.Timestamp, seq uint64) error {
	info := w.ledgerInfos[name]
	if info == nil {
		return fmt.Errorf("invariant: DeleteLedger for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", name)
	}

	info.DeletedAt = deletedAt

	if err := w.saveLedgerInfo(info); err != nil {
		return err
	}

	if err := w.boundary.Delete(w.batch, domain.LedgerKey{Name: name}.Bytes()); err != nil {
		return fmt.Errorf("deleting boundaries for ledger %q: %w", name, err)
	}

	delete(w.boundaries, name)

	// Unlike the rest of the per-ledger data (deferred to the covering
	// purge), the live path deletes the reversion rows at DeleteLedger apply
	// (WriteSet.Merge) — mirror it so the restored store does not resurrect
	// them into Registry.Reversions on boot.
	delete(w.reversions, name)
	delete(w.dirtyReversions, name)

	if err := state.DeleteReversionsByLedger(w.batch, name); err != nil {
		return fmt.Errorf("deleting reversions for ledger %q: %w", name, err)
	}

	return state.SavePendingLedgerCleanup(w.batch, name, seq)
}

// promoteLedger folds a PromoteLedger log onto the ledger's LedgerInfo,
// matching the live handler: mirror mode ends, the mirror source is cleared.
func (w *attributeReplayWriter) promoteLedger(name string) error {
	info := w.ledgerInfos[name]
	if info == nil {
		return fmt.Errorf("invariant: PromoteLedger for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", name)
	}

	info.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL
	info.MirrorSource = nil

	return w.saveLedgerInfo(info)
}

// SetDefaultEnforcementMode folds an enforcement-mode change replayed from the
// log onto the ledger's in-memory LedgerInfo and re-persists it.
func (w *attributeReplayWriter) SetDefaultEnforcementMode(ledger string, mode commonpb.ChartEnforcementMode) error {
	info := w.ledgerInfos[ledger]
	if info == nil {
		return fmt.Errorf("invariant: SetDefaultEnforcementMode for ledger %q with no LedgerInfo seeded from checkpoint or CreateLedger replay", ledger)
	}

	info.DefaultEnforcementMode = mode

	return w.saveLedgerInfo(info)
}

// boundaryFor returns the working LedgerBoundaries for a ledger, seeding it from
// the checkpoint on first touch (a ledger created in the delta has none, so it
// starts at the genesis {1,1}). The returned pointer is mutated in place and
// written back by flushBoundaries.
func (w *attributeReplayWriter) boundaryFor(ledgerName string) (*raftcmdpb.LedgerBoundaries, error) {
	if b, ok := w.boundaries[ledgerName]; ok {
		return b, nil
	}

	existing, err := w.boundary.Get(w.readHandle, domain.LedgerKey{Name: ledgerName}.Bytes())
	if err != nil {
		return nil, fmt.Errorf("reading boundaries for ledger %q: %w", ledgerName, err)
	}

	if existing == nil {
		existing = &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	}

	w.boundaries[ledgerName] = existing

	return existing, nil
}

// initBoundaries resets a ledger's boundaries to the genesis state, matching the
// live CreateLedger handler. A ledger recreated in the delta starts fresh.
func (w *attributeReplayWriter) initBoundaries(ledgerName string) {
	w.boundaries[ledgerName] = &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
}

// advanceLogID bumps NextLogId past a replayed ledger log, mirroring the live
// apply path's per-log increment.
func (w *attributeReplayWriter) advanceLogID(ledgerName string, logID uint64) error {
	b, err := w.boundaryFor(ledgerName)
	if err != nil {
		return err
	}

	if next := logID + 1; next > b.GetNextLogId() {
		b.NextLogId = next
	}

	return nil
}

// recordTransactionBoundary advances a ledger's boundaries for one created
// transaction: NextTransactionId past its id, PostingCount by its postings, and
// RevertCount when it reverts another (revert reversals carry revertsTransaction
// and flow through CreateTransaction like any other tx).
func (w *attributeReplayWriter) recordTransactionBoundary(canonicalKey []byte, postingCount int, revertsTransaction uint64) error {
	var tk domain.TransactionKey
	if err := tk.Unmarshal(canonicalKey); err != nil {
		return fmt.Errorf("parsing transaction key for boundaries: %w", err)
	}

	b, err := w.boundaryFor(tk.LedgerName)
	if err != nil {
		return err
	}

	if next := tk.ID + 1; next > b.GetNextTransactionId() {
		b.NextTransactionId = next
	}

	b.PostingCount += uint64(postingCount)

	if revertsTransaction != 0 {
		b.RevertCount++
	}

	return nil
}

// countNetAttributes sets VolumeCount, MetadataCount and ReferenceCount for
// every touched ledger to the number of persisted keys in the committed 0xF1
// state. These are net (last-value) counts, so counting the final keys is
// exact: ephemeral and transient volumes have already been purged, matching the
// live counters.
//
// NextTransactionId, NextLogId, PostingCount, RevertCount accumulate during
// the log replay; NumscriptExecutionCount and the mirror fill-gap advances come
// from applyAuditOrderEffects. EphemeralEvictedCount / TransientUsedCount are
// carried from the checkpoint unchanged.
func (w *attributeReplayWriter) countNetAttributes(reader dal.PebbleReader) error {
	for name, b := range w.boundaries {
		prefix := domain.LedgerScopedPrefix(name)

		volumeCount, err := countAttributeKeys(w.volume, reader, prefix)
		if err != nil {
			return fmt.Errorf("counting volumes for ledger %q: %w", name, err)
		}

		metadataCount, err := countAttributeKeys(w.metadata, reader, prefix)
		if err != nil {
			return fmt.Errorf("counting metadata for ledger %q: %w", name, err)
		}

		referenceCount, err := countAttributeKeys(w.references, reader, prefix)
		if err != nil {
			return fmt.Errorf("counting references for ledger %q: %w", name, err)
		}

		b.VolumeCount = volumeCount
		b.MetadataCount = metadataCount
		b.ReferenceCount = referenceCount
	}

	return nil
}

// countAttributeKeys counts the persisted keys under a canonical prefix (the
// fixed-width ledger name) in the 0xF1 attribute zone.
func countAttributeKeys[V proto.Message](attr *attributes.Attribute[V], reader dal.PebbleReader, prefix []byte) (uint64, error) {
	si, err := attr.NewStreamingIter(reader, prefix)
	if err != nil {
		return 0, err
	}

	defer func() { _ = si.Close() }()

	var count uint64
	for si.Next() {
		count++
	}

	return count, si.Err()
}

// flushReversions writes the reversion bitset words of every ledger the
// replay reverted in, in the same [zone][sub][ledger][wordIndex] layout the
// FSM persists (SaveReversionWord). Untouched ledgers keep their
// checkpoint-time rows; zero words are skipped — a missing row reads as an
// all-zero word (query.ReadReversions).
func (w *attributeReplayWriter) flushReversions() error {
	for name := range w.dirtyReversions {
		bs := w.reversions[name]
		if bs == nil {
			continue
		}

		for i, word := range bs.Words() {
			if word == 0 {
				continue
			}

			if err := state.SaveReversionWord(w.batch, name, uint64(i), word); err != nil {
				return fmt.Errorf("writing reversion word for ledger %q: %w", name, err)
			}
		}
	}

	return nil
}

// flushBoundaries writes every touched ledger's boundaries to the SubAttrBoundary
// attribute. Called after countNetAttributes, in its own post-commit batch.
func (w *attributeReplayWriter) flushBoundaries() error {
	for name, b := range w.boundaries {
		if _, err := w.boundary.Set(w.batch, domain.LedgerKey{Name: name}.Bytes(), b); err != nil {
			return fmt.Errorf("writing boundaries for ledger %q: %w", name, err)
		}
	}

	return nil
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

func (w *attributeReplayWriter) SetMetadata(canonicalKey []byte, value *commonpb.MetadataValue) error {
	_, err := w.metadata.Set(w.batch, canonicalKey, value)

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

// getTx returns the in-batch state if present, otherwise the committed state.
// Symmetric to GetVolume — required because w.batch is non-indexed and would
// otherwise hide same-batch writes from subsequent reads within the 5000-log
// commit window.
func (w *attributeReplayWriter) getTx(canonicalKey []byte) (*commonpb.TransactionState, error) {
	if state, ok := w.pendingTx[string(canonicalKey)]; ok {
		return state, nil
	}

	return w.tx.Get(w.store, canonicalKey)
}

func (w *attributeReplayWriter) CreateTransaction(canonicalKey []byte, seq uint64, timestamp *commonpb.Timestamp, metadata map[string]*commonpb.MetadataValue, postings []*commonpb.Posting, revertsTransaction uint64) error {
	if err := w.recordTransactionBoundary(canonicalKey, len(postings), revertsTransaction); err != nil {
		return err
	}

	txState := &commonpb.TransactionState{
		CreatedByLog:       seq,
		Metadata:           metadata,
		Timestamp:          timestamp,
		Postings:           postings,
		RevertsTransaction: revertsTransaction,
	}

	if _, err := w.tx.Set(w.batch, canonicalKey, txState); err != nil {
		return err
	}

	w.pendingTx[string(canonicalKey)] = txState

	return nil
}

// SetTransactionReference reconstructs the reference→txID uniqueness index in
// the SubAttrReference attribute, so reference idempotency is enforced against
// delta transactions after a restore.
func (w *attributeReplayWriter) SetTransactionReference(ledgerName, reference string, txID uint64) error {
	key := domain.TransactionReferenceKey{LedgerName: ledgerName, Reference: reference}.Bytes()

	_, err := w.references.Set(w.batch, key, &commonpb.TransactionReferenceValue{TransactionId: txID})

	return err
}

func (w *attributeReplayWriter) SetRevertedBy(canonicalKey []byte, revertTxID uint64, revertedAt *commonpb.Timestamp) error {
	existing, err := w.getTx(canonicalKey)
	if err != nil {
		return err
	}

	if existing == nil {
		existing = &commonpb.TransactionState{}
	}

	existing.RevertedByTransaction = revertTxID
	existing.RevertedAt = revertedAt

	if _, err := w.tx.Set(w.batch, canonicalKey, existing); err != nil {
		return err
	}

	w.pendingTx[string(canonicalKey)] = existing

	// Fold the reverted id into the ledger's reversion bitset — the structure
	// the FSM's already-reverted gate actually reads.
	var tk domain.TransactionKey
	if err := tk.Unmarshal(canonicalKey); err != nil {
		return fmt.Errorf("unmarshaling reverted transaction key: %w", err)
	}

	bs := w.reversions[tk.LedgerName]
	if bs == nil {
		bs = &bitset.Bitset{}
		w.reversions[tk.LedgerName] = bs
	}

	bs.Set(tk.ID)
	w.dirtyReversions[tk.LedgerName] = struct{}{}

	return nil
}

func (w *attributeReplayWriter) SaveTxMetadata(canonicalKey []byte, metadata map[string]*commonpb.MetadataValue) error {
	existing, err := w.getTx(canonicalKey)
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

	if _, err := w.tx.Set(w.batch, canonicalKey, existing); err != nil {
		return err
	}

	w.pendingTx[string(canonicalKey)] = existing

	return nil
}

func (w *attributeReplayWriter) DeleteTxMetadata(canonicalKey []byte, key string) error {
	existing, err := w.getTx(canonicalKey)
	if err != nil {
		return err
	}

	if existing == nil || existing.GetMetadata() == nil {
		return nil
	}

	delete(existing.GetMetadata(), key)

	if _, err := w.tx.Set(w.batch, canonicalKey, existing); err != nil {
		return err
	}

	w.pendingTx[string(canonicalKey)] = existing

	return nil
}
