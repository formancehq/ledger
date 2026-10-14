package state

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type inventoryDelta struct {
	volumes  int64
	metadata int64
	changed  bool
}

// writeAccountInventories updates the technical key-set projection once per
// account whose final persistent key set changed. Value-only writes do not
// alter the revision: their newer cache entry shadows an older preload.
func (b *WriteSet) writeAccountInventories(
	batch *dal.WriteSession,
	genByte byte,
	kept, purged []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	volumeDeletions []attributes.Deletion[domain.VolumeKey],
	metadataUpdates []attributes.Update[domain.MetadataKey, *commonpb.MetadataValue],
	metadataDeletions []attributes.Deletion[domain.MetadataKey],
) error {
	deltas := make(map[domain.AccountKey]inventoryDelta)
	for _, update := range kept {
		if !update.Old.IsDefined() {
			d := deltas[update.Key.AccountKey]
			d.volumes++
			d.changed = true
			deltas[update.Key.AccountKey] = d
		}
	}
	for _, update := range purged {
		if update.Old.IsDefined() {
			d := deltas[update.Key.AccountKey]
			d.volumes--
			d.changed = true
			deltas[update.Key.AccountKey] = d
		}
	}
	for _, deletion := range volumeDeletions {
		d := deltas[deletion.Key.AccountKey]
		d.volumes--
		d.changed = true
		deltas[deletion.Key.AccountKey] = d
	}
	for _, update := range metadataUpdates {
		if !update.Old.IsDefined() {
			d := deltas[update.Key.AccountKey]
			d.metadata++
			d.changed = true
			deltas[update.Key.AccountKey] = d
		}
	}
	for _, deletion := range metadataDeletions {
		d := deltas[deletion.Key.AccountKey]
		d.metadata--
		d.changed = true
		deltas[deletion.Key.AccountKey] = d
	}

	accounts := make([]domain.AccountKey, 0, len(deltas))
	for account, delta := range deltas {
		if delta.changed {
			accounts = append(accounts, account)
		}
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].LedgerName != accounts[j].LedgerName {
			return accounts[i].LedgerName < accounts[j].LedgerName
		}

		return accounts[i].Account < accounts[j].Account
	})

	for _, account := range accounts {
		old, _, err := b.fsm.Registry.AccountInventories.GetKey(account)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("reading account inventory %q: %w", account.Account, err)
		}
		var oldVolumes, oldMetadata uint64
		if err == nil {
			oldVolumes = uint64(old.GetVolumeCount())
			oldMetadata = uint64(old.GetMetadataCount())
		}
		delta := deltas[account]
		volumes := int64(oldVolumes) + delta.volumes
		metadata := int64(oldMetadata) + delta.metadata
		if volumes < 0 || metadata < 0 || volumes > math.MaxUint32 || metadata > math.MaxUint32 {
			return fmt.Errorf("invariant: invalid account inventory counts for %q: volumes=%d metadata=%d", account.Account, volumes, metadata)
		}
		if volumes == 0 && metadata == 0 {
			if old != nil {
				if err := b.attrs.AccountInventory.Delete(batch, account.Bytes()); err != nil {
					return err
				}
				if err := b.fsm.Registry.AccountInventories.TombstoneCacheOnly(batch, genByte, account.Bytes()); err != nil {
					return err
				}
			}

			continue
		}
		birth := b.fsm.State.LastAppliedIndex
		if old != nil {
			birth = old.GetBirthIndex()
		}
		_, _, err = b.fsm.Registry.AccountInventories.PutWithCache(batch, genByte, account.Bytes(), &raftcmdpb.AccountInventoryState{
			BirthIndex:        birth,
			InventoryRevision: b.fsm.State.LastAppliedIndex,
			VolumeCount:       uint32(volumes),
			MetadataCount:     uint32(metadata),
		})
		if err != nil {
			return fmt.Errorf("writing account inventory %q: %w", account.Account, err)
		}
	}

	return nil
}
