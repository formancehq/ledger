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
// proposal. It opens or updates one incarnation per touched normal account,
// regardless of the number of assets or metadata keys. Ephemeral and
// transient lifecycles are deliberately unchanged in this experiment.
func (b *WriteSet) ensureAccountIncarnations(scope processing.Scope) domain.SerializableError {
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
		if matched := accounttype.FindMatchingType(key.Account, ledgerType.compiled); matched != nil &&
			matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL {
			continue
		}
		if err := scope.CheckCoverage(dal.SubAttrAccountIncarnation, key); err != nil {
			return domain.StoreFailure("checking account incarnation coverage", err)
		}
		if incarnation, err := b.Derived.AccountIncarnations.Get(key); err == nil {
			if incarnation.GetUpdatedAt().GetData() != date.GetData() {
				updated := incarnation.CloneVT()
				updated.UpdatedAt = date.CloneVT()
				b.Derived.AccountIncarnations.Put(key, updated)
			}

			continue
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.StoreFailure("reading account incarnation", err)
		}
		b.Derived.AccountIncarnations.Put(key, &commonpb.AccountIncarnation{
			Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
			InsertedAt:  date.CloneVT(),
			UpdatedAt:   date.CloneVT(),
		})
	}

	return nil
}
