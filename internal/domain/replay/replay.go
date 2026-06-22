package replay

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ReplayLedgerLog updates expected state in the writer based on a ledger log payload.
// rawLedgerTypes tracks the raw account type map for add/remove mutations.
// ledgerAccountTypes tracks pre-compiled account types for ephemeral purge simulation.
//
// Extracted from internal/application/check/checker.go for reuse across
// the integrity checker and the backup restore pipeline.
func ReplayLedgerLog(
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	w Writer,
	rawLedgerTypes map[string]map[string]*commonpb.AccountType,
	ledgerAccountTypes map[string][]accounttype.CompiledType,
	ephemeralPurgeBuffer *EphemeralPurgeBuffer,
) error {
	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_AddedAccountType:
		if p.AddedAccountType != nil && p.AddedAccountType.GetAccountType() != nil {
			at := p.AddedAccountType.GetAccountType()
			types := rawLedgerTypes[ledger]
			if types == nil {
				types = make(map[string]*commonpb.AccountType)
				rawLedgerTypes[ledger] = types
			}

			types[at.GetName()] = at
			ledgerAccountTypes[ledger] = accounttype.CompileTypes(types)
		}

	case *commonpb.LedgerLogPayload_RemovedAccountType:
		if p.RemovedAccountType != nil {
			if types := rawLedgerTypes[ledger]; types != nil {
				delete(types, p.RemovedAccountType.GetName())
				ledgerAccountTypes[ledger] = accounttype.CompileTypes(types)
			}
		}

	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction == nil || p.CreatedTransaction.GetTransaction() == nil {
			return nil
		}

		tx := p.CreatedTransaction.GetTransaction()
		if err := ApplyPostings(ledger, tx.GetPostings(), w); err != nil {
			return err
		}

		if err := replayEphemeralPurge(ledger, tx.GetPostings(), w, ledgerAccountTypes, ephemeralPurgeBuffer); err != nil {
			return err
		}

		txCanonical := domain.TransactionKey{LedgerName: ledger, ID: tx.GetId()}.Bytes()

		if err := w.CreateTransaction(txCanonical, seq, tx.GetTimestamp(), tx.GetMetadata()); err != nil {
			return fmt.Errorf("putting tx state for created tx %d: %w", tx.GetId(), err)
		}

		for account, metadataMap := range p.CreatedTransaction.GetAccountMetadata() {
			if metadataMap != nil {
				for key, value := range metadataMap.GetValues() {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							LedgerName: ledger,
							Account:    account,
						},
						Key: key,
					}

					if value != nil {
						if err := w.SetMetadata(mk.Bytes(), commonpb.MetadataValueToString(value)); err != nil {
							return fmt.Errorf("setting account metadata: %w", err)
						}
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil || p.RevertedTransaction.GetRevertTransaction() == nil {
			return nil
		}

		revertTx := p.RevertedTransaction.GetRevertTransaction()
		if err := ApplyPostings(ledger, revertTx.GetPostings(), w); err != nil {
			return err
		}

		if err := replayEphemeralPurge(ledger, revertTx.GetPostings(), w, ledgerAccountTypes, ephemeralPurgeBuffer); err != nil {
			return err
		}

		origTxCanonical := domain.TransactionKey{LedgerName: ledger, ID: p.RevertedTransaction.GetRevertedTransactionId()}.Bytes()

		if err := w.SetRevertedBy(origTxCanonical, revertTx.GetId()); err != nil {
			return fmt.Errorf("putting revert marker for tx %d: %w", p.RevertedTransaction.GetRevertedTransactionId(), err)
		}

		revertTxCanonical := domain.TransactionKey{LedgerName: ledger, ID: revertTx.GetId()}.Bytes()

		if err := w.CreateTransaction(revertTxCanonical, seq, revertTx.GetTimestamp(), revertTx.GetMetadata()); err != nil {
			return fmt.Errorf("putting tx state for revert tx %d: %w", revertTx.GetId(), err)
		}

	case *commonpb.LedgerLogPayload_SavedMetadata:
		if p.SavedMetadata == nil || p.SavedMetadata.GetTarget() == nil {
			return nil
		}

		switch target := p.SavedMetadata.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			if len(p.SavedMetadata.GetMetadata()) > 0 {
				for key, value := range p.SavedMetadata.GetMetadata() {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							LedgerName: ledger,
							Account:    target.Account.GetAddr(),
						},
						Key: key,
					}

					if value != nil {
						if err := w.SetMetadata(mk.Bytes(), commonpb.MetadataValueToString(value)); err != nil {
							return fmt.Errorf("setting metadata: %w", err)
						}
					}
				}
			}
		case *commonpb.Target_TransactionId:
			if len(p.SavedMetadata.GetMetadata()) > 0 {
				txCanonical := domain.TransactionKey{LedgerName: ledger, ID: target.TransactionId}.Bytes()

				if err := w.SaveTxMetadata(txCanonical, p.SavedMetadata.GetMetadata()); err != nil {
					return fmt.Errorf("saving tx metadata for tx %d: %w", target.TransactionId, err)
				}
			}
		}

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		if p.DeletedMetadata == nil || p.DeletedMetadata.GetTarget() == nil {
			return nil
		}

		switch target := p.DeletedMetadata.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			mk := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					LedgerName: ledger,
					Account:    target.Account.GetAddr(),
				},
				Key: p.DeletedMetadata.GetKey(),
			}

			if err := w.DeleteMetadata(mk.Bytes()); err != nil {
				return fmt.Errorf("deleting metadata: %w", err)
			}
		case *commonpb.Target_TransactionId:
			txCanonical := domain.TransactionKey{LedgerName: ledger, ID: target.TransactionId}.Bytes()

			if err := w.DeleteTxMetadata(txCanonical, p.DeletedMetadata.GetKey()); err != nil {
				return fmt.Errorf("deleting tx metadata for tx %d: %w", target.TransactionId, err)
			}
		}

	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema operations — no state to track
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		// Schema operations — no state to track
	case *commonpb.LedgerLogPayload_FillGap:
		// No state to track
	case *commonpb.LedgerLogPayload_CreateIndex:
		// Index operations — no state to track
	case *commonpb.LedgerLogPayload_DropIndex:
		// Index operations — no state to track
	case *commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode:
		// No state to track
	}

	return nil
}

type pendingEphemeralPurge struct {
	postings []*commonpb.Posting
}

// EphemeralPurgeBuffer accumulates transaction postings until the caller reaches
// the same proposal boundary used by the FSM's WriteSet.Merge().
type EphemeralPurgeBuffer struct {
	byLedger map[string]*pendingEphemeralPurge
	ledgers  []string
}

// NewEphemeralPurgeBuffer creates a buffer for replay callers that know batch
// boundaries, such as the integrity checker via audit entries.
func NewEphemeralPurgeBuffer() *EphemeralPurgeBuffer {
	return &EphemeralPurgeBuffer{
		byLedger: make(map[string]*pendingEphemeralPurge),
	}
}

// Add records postings for a ledger in the current replay batch.
func (b *EphemeralPurgeBuffer) Add(ledger string, postings []*commonpb.Posting) {
	if b == nil || len(postings) == 0 {
		return
	}

	pending := b.byLedger[ledger]
	if pending == nil {
		pending = &pendingEphemeralPurge{}
		b.byLedger[ledger] = pending
		b.ledgers = append(b.ledgers, ledger)
	}

	pending.postings = append(pending.postings, postings...)
}

// Flush applies the accumulated purge decisions once per replay batch.
func (b *EphemeralPurgeBuffer) Flush(
	w Writer,
	ledgerAccountTypes map[string][]accounttype.CompiledType,
) error {
	if b == nil || len(b.byLedger) == 0 {
		return nil
	}

	for _, ledger := range b.ledgers {
		pending := b.byLedger[ledger]
		if err := SimulateEphemeralPurge(ledger, pending.postings, w, ledgerAccountTypes); err != nil {
			return err
		}
	}

	clear(b.byLedger)
	b.ledgers = b.ledgers[:0]

	return nil
}

func replayEphemeralPurge(
	ledger string,
	postings []*commonpb.Posting,
	w Writer,
	ledgerAccountTypes map[string][]accounttype.CompiledType,
	ephemeralPurgeBuffer *EphemeralPurgeBuffer,
) error {
	if ephemeralPurgeBuffer != nil {
		ephemeralPurgeBuffer.Add(ledger, postings)

		return nil
	}

	return SimulateEphemeralPurge(ledger, postings, w, ledgerAccountTypes)
}

// ProposalBoundaryTracker filters audit log ranges down to newly-created log
// boundaries. Audit ranges may include idempotent references to older logs; only
// a monotonically advancing max log sequence can close a replayed proposal.
type ProposalBoundaryTracker struct {
	lastLogSequence uint64
}

// NewProposalBoundaryTracker creates a tracker seeded with the highest log
// sequence already covered by the replay base, such as an archived chapter or a
// backup checkpoint.
func NewProposalBoundaryTracker(replayedThrough uint64) *ProposalBoundaryTracker {
	return &ProposalBoundaryTracker{lastLogSequence: replayedThrough}
}

// Accept returns true when maxLogSequence represents a new proposal boundary.
func (t *ProposalBoundaryTracker) Accept(maxLogSequence uint64) (uint64, bool) {
	if t == nil || maxLogSequence == 0 || maxLogSequence <= t.lastLogSequence {
		return 0, false
	}

	t.lastLogSequence = maxLogSequence

	return maxLogSequence, true
}

// ApplyPostings applies postings to the writer as volume deltas.
func ApplyPostings(
	ledger string,
	postings []*commonpb.Posting,
	w Writer,
) error {
	for _, posting := range postings {
		amount := posting.GetAmount().ToBigInt()

		sourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				LedgerName: ledger,
				Account:    posting.GetSource(),
			},
			Asset: posting.GetAsset(),
		}

		if err := w.AddVolumeDelta(sourceKey.Bytes(), big.NewInt(0), amount); err != nil {
			return fmt.Errorf("adding source volume delta: %w", err)
		}

		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				LedgerName: ledger,
				Account:    posting.GetDestination(),
			},
			Asset: posting.GetAsset(),
		}

		if err := w.AddVolumeDelta(destKey.Bytes(), amount, big.NewInt(0)); err != nil {
			return fmt.Errorf("adding dest volume delta: %w", err)
		}
	}

	return nil
}

// SimulateEphemeralPurge checks if any account volumes affected by the postings
// have reached zero balance (input == output) on an ephemeral account type.
// If so, it deletes the volume, mirroring the real purge in WriteSet.Merge().
func SimulateEphemeralPurge(
	ledger string,
	postings []*commonpb.Posting,
	w Writer,
	ledgerAccountTypes map[string][]accounttype.CompiledType,
) error {
	compiled := ledgerAccountTypes[ledger]
	if len(compiled) == 0 {
		return nil
	}

	seen := make(map[string]struct{})

	for _, posting := range postings {
		for _, addr := range []string{posting.GetSource(), posting.GetDestination()} {
			if addr == "world" {
				continue
			}

			if _, ok := seen[addr]; ok {
				continue
			}

			seen[addr] = struct{}{}

			matched := accounttype.FindMatchingType(addr, compiled)
			if matched == nil || matched.GetPersistence() == commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL {
				continue
			}

			for _, p := range postings {
				if p.GetSource() != addr && p.GetDestination() != addr {
					continue
				}

				vk := domain.VolumeKey{
					AccountKey: domain.AccountKey{LedgerName: ledger, Account: addr},
					Asset:      p.GetAsset(),
				}

				pair, err := w.GetVolume(vk.Bytes())
				if err != nil {
					return fmt.Errorf("reading volume for ephemeral check: %w", err)
				}

				if pair == nil {
					continue
				}

				inBig := pair.GetInput().ToBigInt()
				outBig := pair.GetOutput().ToBigInt()

				if inBig.Cmp(outBig) == 0 {
					if err := w.DeleteVolume(vk.Bytes()); err != nil {
						return fmt.Errorf("deleting ephemeral volume: %w", err)
					}
				}
			}
		}
	}

	return nil
}
