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

// ensureAccountIncarnations is the benchmark's shadow write path. It observes
// the account attribute once per account touched by a posting and creates a
// stable incarnation for normal accounts. Ephemeral and transient lifecycles
// are deliberately unchanged in this experiment.
func (b *WriteSet) ensureAccountIncarnations(scope processing.Scope) domain.SerializableError {
	seen := make(map[domain.AccountKey]struct{})
	for volume := range b.Derived.Volumes.DirtyValues() {
		if volume.Account == "world" {
			continue
		}
		seen[volume.AccountKey] = struct{}{}
	}
	accounts := make([]domain.AccountKey, 0, len(seen))
	for key := range seen {
		accounts = append(accounts, key)
	}
	slices.SortFunc(accounts, func(a, c domain.AccountKey) int {
		return cmp.Or(cmp.Compare(a.LedgerName, c.LedgerName), cmp.Compare(a.Account, c.Account))
	})

	for _, key := range accounts {
		ledgerType, ok := b.gatedLedgerTypes[key.LedgerName]
		if !ok || !ledgerType.found {
			continue
		}
		if matched := accounttype.FindMatchingType(key.Account, ledgerType.compiled); matched != nil &&
			matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL {
			continue
		}
		if err := scope.CheckCoverage(dal.SubAttrAccountIncarnation, key); err != nil {
			return domain.StoreFailure("checking account incarnation coverage", err)
		}
		if _, err := b.Derived.AccountIncarnations.Get(key); err == nil {
			continue
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.StoreFailure("reading account incarnation", err)
		}
		b.Derived.AccountIncarnations.Put(key, &commonpb.AccountIncarnation{
			OpenedAtRaftIndex: scope.GetRaftIndex(),
			Persistence:       commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL,
		})
	}
	return nil
}
