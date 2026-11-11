package state

import (
	"cmp"
	"errors"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// assertAccountIncarnations compares admission's observed account lifetimes
// with the FSM cache after Preload and before any order can mutate the write
// set. A concurrent opening makes an absent observation stale; future close
// and reopen operations will use the same assertion to reject an old lifetime.
func (fsm *Machine) assertAccountIncarnations(plan *raftcmdpb.ExecutionPlan) domain.SerializableError {
	for _, coverage := range plan.GetAttributes() {
		if !coverage.GetAssertAccountIncarnation() {
			continue
		}
		id := attributes.U128FromBytes(coverage.GetId().GetId())
		current := uint64(0)
		if entry, ok := fsm.Registry.Cache.AccountIncarnations.Get(id); ok && !entry.Deleted && entry.Data != nil {
			current = entry.Data.GetOpenedAtRaftIndex()
		}
		if current != coverage.GetExpectedIncarnationOpenedAtRaftIndex() {
			return domain.ErrStaleInputsResolution
		}
	}

	return nil
}

// ensureAccountIncarnations observes each account changed by a posting or an
// account-metadata write and opens a stable incarnation for normal accounts.
// Metadata deletions alone do not open an account. Ephemeral and transient
// lifecycles are deliberately unchanged in this experiment.
func (b *WriteSet) ensureAccountIncarnations(scope processing.Scope) domain.SerializableError {
	seen := make(map[domain.AccountKey]struct{})
	for volume := range b.Derived.Volumes.DirtyValues() {
		if volume.Account == "world" {
			continue
		}
		seen[volume.AccountKey] = struct{}{}
	}
	for metadata := range b.Derived.AccountMetadata.DirtyValues() {
		if metadata.Account == "world" {
			continue
		}
		seen[metadata.AccountKey] = struct{}{}
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
