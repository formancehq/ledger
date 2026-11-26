package state

import (
	"cmp"
	"errors"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ensureAccountIncarnations observes the final account write set for this
// proposal. It opens or updates one incarnation per touched normal or ephemeral
// account, regardless of the number of assets or metadata keys.
func (b *WriteSet) ensureAccountIncarnations(scope processing.Scope) domain.SerializableError {
	b.incarnationsEnabled = true
	seen := make(map[domain.AccountKey]struct{})
	for volume := range b.Derived.Volumes.DirtyValues() {
		seen[volume.AccountKey] = struct{}{}
	}
	for volume := range b.Derived.Volumes.DirtyDeletions() {
		seen[volume.AccountKey] = struct{}{}
	}
	for metadata := range b.Derived.AccountMetadata.DirtyValues() {
		seen[metadata.AccountKey] = struct{}{}
	}
	for metadata := range b.Derived.AccountMetadata.DirtyDeletions() {
		seen[metadata.AccountKey] = struct{}{}
	}
	accounts := make([]domain.AccountKey, 0, len(seen))
	for key := range seen {
		accounts = append(accounts, key)
	}
	slices.SortFunc(accounts, func(a, c domain.AccountKey) int {
		return cmp.Or(cmp.Compare(a.LedgerName, c.LedgerName), cmp.Compare(a.Account, c.Account))
	})
	if len(accounts) == 0 {
		return nil
	}
	date := scope.GetDate().Mutate()

	for _, key := range accounts {
		ledgerType, ok := b.gatedLedgerTypes[key.LedgerName]
		if !ok {
			info, err := scope.Ledgers().Get(domain.LedgerKey{Name: key.LedgerName})
			if errors.Is(err, domain.ErrNotFound) {
				b.gatedLedgerTypes[key.LedgerName] = gatedLedgerType{}

				continue
			}
			if err != nil {
				return domain.StoreFailure("loading ledger for account incarnation", err)
			}
			ledgerType = gatedLedgerType{
				compiled: accounttype.CompileTypes(info.Mutate().GetAccountTypes()),
				found:    true,
			}
			b.gatedLedgerTypes[key.LedgerName] = ledgerType
		}
		if !ledgerType.found {
			continue
		}
		persistence := commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL
		if matched := accounttype.FindMatchingType(key.Account, ledgerType.compiled); matched != nil {
			persistence = matched.GetPersistence()
		}
		if persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			continue
		}
		if err := scope.CheckCoverage(dal.SubAttrAccountIncarnation, key); err != nil {
			return domain.StoreFailure("checking account incarnation coverage", err)
		}
		if incarnation, err := b.Derived.AccountIncarnations.Get(key); err == nil {
			if incarnation.GetUpdatedAt().GetData() != date.GetData() || incarnation.GetPersistence() != persistence {
				updated := incarnation.CloneVT()
				updated.UpdatedAt = date.CloneVT()
				updated.Persistence = persistence
				b.Derived.AccountIncarnations.Put(key, updated)
			}

			continue
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.StoreFailure("reading account incarnation", err)
		}
		b.Derived.AccountIncarnations.Put(key, &commonpb.AccountIncarnation{
			Persistence: persistence,
			InsertedAt:  date.CloneVT(),
			UpdatedAt:   date.CloneVT(),
		})
	}

	return nil
}

// adjustIncarnationVolumeCounts applies the final persisted-volume partition.
// The count tracks all (asset, color) cells, so closing one asset cannot close
// an ephemeral account that still owns another.
func (b *WriteSet) adjustIncarnationVolumeCounts(part volumePartitionResult) error {
	deltas := make(map[domain.AccountKey]int64)
	for _, update := range part.kept {
		if !update.Old.IsDefined() {
			deltas[update.Key.AccountKey]++
		}
	}
	for _, update := range part.purged {
		if update.Old.IsDefined() {
			deltas[update.Key.AccountKey]--
		} else {
			// A newly created and purged cell still closes an empty incarnation.
			deltas[update.Key.AccountKey] += 0
		}
	}
	for key, delta := range deltas {
		row, err := b.Derived.AccountIncarnations.Get(key)
		if errors.Is(err, domain.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if delta < 0 && uint64(-delta) > row.GetVolumeCount() {
			return errors.New("account incarnation volume count underflow")
		}
		count := row.GetVolumeCount()
		if delta < 0 {
			count -= uint64(-delta)
		} else {
			count += uint64(delta)
		}
		if row.GetPersistence() == commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL && count == 0 {
			b.Derived.AccountIncarnations.Delete(key)

			continue
		}
		if count != row.GetVolumeCount() {
			updated := row.CloneVT()
			updated.VolumeCount = count
			b.Derived.AccountIncarnations.Put(key, updated)
		}
	}

	return nil
}
