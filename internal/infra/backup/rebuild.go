package backup

import (
	"context"
	"fmt"
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/replay"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// RebuildDelta reconstructs derived state (attributes, system state) from logs
// that were added by export segments beyond the checkpoint.
// If fromLogSeq is 0, it rebuilds from the very first log (full rebuild).
func RebuildDelta(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	fromLogSeq uint64,
) error {
	attrs := attributes.New()
	batch := store.NewBatch()

	writer := &attributeReplayWriter{
		store:    store,
		batch:    batch,
		volume:   attrs.Volume,
		metadata: attrs.Metadata,
		tx:       attrs.Transaction,
	}

	sinkConfig := attrs.SinkConfig
	numscriptContent := attrs.NumscriptContent
	numscriptVersion := attrs.NumscriptVersion

	rawLedgerTypes := make(map[string]map[string]*commonpb.AccountType)
	ledgerAccountTypes := make(map[string][]accounttype.CompiledType)

	logCursor, err := query.ReadLogsSince(ctx, store, fromLogSeq)
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("reading logs since %d: %w", fromLogSeq, err)
	}

	defer func() { _ = logCursor.Close() }()

	var count uint64

	for {
		log, err := logCursor.Next()
		if err != nil {
			break // io.EOF or error
		}

		payload := log.GetPayload()
		seq := log.GetSequence()

		switch p := payload.GetType().(type) {
		case *commonpb.LogPayload_Apply:
			if p.Apply == nil || p.Apply.GetLog() == nil || p.Apply.GetLog().GetData() == nil {
				continue
			}

			ledgerName := p.Apply.GetLedgerName()

			if err := replay.ReplayLedgerLog(ledgerName, seq, p.Apply.GetLog().GetData(), writer, rawLedgerTypes, ledgerAccountTypes); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("replaying ledger log %d: %w", seq, err)
			}

		case *commonpb.LogPayload_CreateLedger:
			if p.CreateLedger == nil {
				continue
			}

			info := &commonpb.LedgerInfo{
				Name:      p.CreateLedger.GetName(),
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

		case *commonpb.LogPayload_SetPeriodSchedule:
			if p.SetPeriodSchedule != nil {
				if err := state.SavePeriodSchedule(batch, p.SetPeriodSchedule.GetCron()); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving period schedule at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_SavedNumscript:
			if p.SavedNumscript != nil && p.SavedNumscript.GetInfo() != nil {
				info := p.SavedNumscript.GetInfo()
				entryKey := domain.NumscriptEntryKey{Ledger: info.GetLedger(), Name: info.GetName(), Version: info.GetVersion()}
				if _, err := numscriptContent.Set(batch, entryKey.Bytes(), info); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving numscript at log %d: %w", seq, err)
				}

				versionKey := domain.NumscriptVersionKey{Ledger: info.GetLedger(), Name: info.GetName()}
				versionVal := &commonpb.NumscriptVersionValue{Version: info.GetVersion()}
				if _, err := numscriptVersion.Set(batch, versionKey.Bytes(), versionVal); err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("saving numscript version at log %d: %w", seq, err)
				}
			}

		case *commonpb.LogPayload_CreatedPreparedQuery:
			if p.CreatedPreparedQuery != nil && p.CreatedPreparedQuery.GetQuery() != nil {
				if err := state.SavePreparedQuery(batch, p.CreatedPreparedQuery.GetQuery()); err != nil {
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
		case *commonpb.LogPayload_ClosePeriod:
		case *commonpb.LogPayload_SealPeriod:
		case *commonpb.LogPayload_ArchivePeriod:
		case *commonpb.LogPayload_ConfirmArchivePeriod:
		case *commonpb.LogPayload_DeletePeriodSchedule:
		case *commonpb.LogPayload_DeletedPreparedQuery:
		case *commonpb.LogPayload_DeletedNumscript:
		case *commonpb.LogPayload_CreatedQueryCheckpoint:
		case *commonpb.LogPayload_DeletedQueryCheckpoint:
		case *commonpb.LogPayload_DeleteQueryCheckpointSchedule:
		}

		count++

		// Commit in batches to avoid unbounded memory
		if count%5000 == 0 {
			if err := batch.Commit(); err != nil {
				return fmt.Errorf("committing batch at log %d: %w", seq, err)
			}

			batch = store.NewBatch()
			writer.batch = batch

			logger.WithFields(map[string]any{
				"logsProcessed": count,
				"currentSeq":    seq,
			}).Infof("RebuildDelta progress")
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

// attributeReplayWriter implements replay.Writer by writing directly to
// Pebble attributes via Attribute[V].Set/Get/Delete.
type attributeReplayWriter struct {
	store    *dal.Store
	batch    *dal.Batch
	volume   *attributes.Attribute[*raftcmdpb.VolumePair]
	metadata *attributes.Attribute[*commonpb.MetadataValue]
	tx       *attributes.Attribute[*commonpb.TransactionState]
}

func (w *attributeReplayWriter) AddVolumeDelta(canonicalKey []byte, inputDelta, outputDelta *big.Int) error {
	existing, err := w.volume.Get(w.store, canonicalKey)
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

	return err
}

func (w *attributeReplayWriter) GetVolume(canonicalKey []byte) (*raftcmdpb.VolumePair, error) {
	return w.volume.Get(w.store, canonicalKey)
}

func (w *attributeReplayWriter) DeleteVolume(canonicalKey []byte) error {
	return w.volume.Delete(w.batch, canonicalKey)
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

func (w *attributeReplayWriter) CreateTransaction(canonicalKey []byte, seq uint64, metadata map[string]*commonpb.MetadataValue) error {
	txState := &commonpb.TransactionState{
		CreatedByLog: seq,
		Metadata:     metadata,
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
