package replay

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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

		if err := SimulateEphemeralPurge(ledger, tx.GetPostings(), w, ledgerAccountTypes); err != nil {
			return err
		}

		txCanonical := domain.TransactionKey{Ledger: ledger, ID: tx.GetId()}.Bytes()

		if err := w.CreateTransaction(txCanonical, seq, tx.GetMetadata()); err != nil {
			return fmt.Errorf("putting tx state for created tx %d: %w", tx.GetId(), err)
		}

		for account, metadataMap := range p.CreatedTransaction.GetAccountMetadata() {
			if metadataMap != nil {
				for key, value := range metadataMap.GetValues() {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							Ledger:  ledger,
							Account: account,
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

		if err := SimulateEphemeralPurge(ledger, revertTx.GetPostings(), w, ledgerAccountTypes); err != nil {
			return err
		}

		origTxCanonical := domain.TransactionKey{Ledger: ledger, ID: p.RevertedTransaction.GetRevertedTransactionId()}.Bytes()

		if err := w.SetRevertedBy(origTxCanonical, revertTx.GetId()); err != nil {
			return fmt.Errorf("putting revert marker for tx %d: %w", p.RevertedTransaction.GetRevertedTransactionId(), err)
		}

		revertTxCanonical := domain.TransactionKey{Ledger: ledger, ID: revertTx.GetId()}.Bytes()

		if err := w.CreateTransaction(revertTxCanonical, seq, revertTx.GetMetadata()); err != nil {
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
							Ledger:  ledger,
							Account: target.Account.GetAddr(),
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
		case *commonpb.Target_Transaction:
			if len(p.SavedMetadata.GetMetadata()) > 0 {
				txCanonical := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes()

				if err := w.SaveTxMetadata(txCanonical, p.SavedMetadata.GetMetadata()); err != nil {
					return fmt.Errorf("saving tx metadata for tx %d: %w", target.Transaction.GetId(), err)
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
					Ledger:  ledger,
					Account: target.Account.GetAddr(),
				},
				Key: p.DeletedMetadata.GetKey(),
			}

			if err := w.DeleteMetadata(mk.Bytes()); err != nil {
				return fmt.Errorf("deleting metadata: %w", err)
			}
		case *commonpb.Target_Transaction:
			txCanonical := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes()

			if err := w.DeleteTxMetadata(txCanonical, p.DeletedMetadata.GetKey()); err != nil {
				return fmt.Errorf("deleting tx metadata for tx %d: %w", target.Transaction.GetId(), err)
			}
		}

	case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
		if p.ConvertMetadataBatch != nil {
			for _, entry := range p.ConvertMetadataBatch.GetEntries() {
				valueStr := commonpb.MetadataValueToString(entry.GetNewValue())
				if err := w.SetMetadata(entry.GetCanonicalKey(), valueStr); err != nil {
					return fmt.Errorf("replaying metadata conversion: %w", err)
				}
			}
		}

	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema operations — no state to track
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		// Schema operations — no state to track
	case *commonpb.LedgerLogPayload_MetadataConversionComplete:
		// Background conversion — no state to track
	case *commonpb.LedgerLogPayload_FillGap:
		// No state to track
	case *commonpb.LedgerLogPayload_CreateIndex:
		// Index operations — no state to track
	case *commonpb.LedgerLogPayload_DropIndex:
		// Index operations — no state to track
	case *commonpb.LedgerLogPayload_IndexReady:
		// Index operations — no state to track
	case *commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode:
		// No state to track
	}

	return nil
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
				Ledger:  ledger,
				Account: posting.GetSource(),
			},
			Asset: posting.GetAsset(),
		}

		if err := w.AddVolumeDelta(sourceKey.Bytes(), big.NewInt(0), amount); err != nil {
			return fmt.Errorf("adding source volume delta: %w", err)
		}

		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.GetDestination(),
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
// If so, it deletes the volume, mirroring the real purge in Buffered.Merge().
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
					AccountKey: domain.AccountKey{Ledger: ledger, Account: addr},
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
