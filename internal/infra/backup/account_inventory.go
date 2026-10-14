package backup

import (
	"fmt"
	"math"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// rebuildAccountInventories reconstructs the technical key-set projection
// after a full or incremental audit replay. Restored caches are rebuilt from
// the resulting attribute zone; the next Raft write advances revision from 0.
func rebuildAccountInventories(store *dal.Store) error {
	reader, err := store.NewReadHandle()
	if err != nil {
		return err
	}
	readerOpen := true
	defer func() {
		if readerOpen {
			_ = reader.Close()
		}
	}()
	counts := make(map[domain.AccountKey]*raftcmdpb.AccountInventoryState)
	for _, code := range []byte{dal.SubAttrVolume, dal.SubAttrMetadata} {
		iter, err := dal.NewBoundedIter(reader, []byte{dal.ZoneAttributes, code}, []byte{dal.ZoneAttributes, code + 1})
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			var account domain.AccountKey
			if code == dal.SubAttrVolume {
				var key domain.VolumeKey
				if err := key.Unmarshal(iter.Key()[2:]); err != nil {
					_ = iter.Close()

					return err
				}
				account = key.AccountKey
			} else {
				var key domain.MetadataKey
				if err := key.Unmarshal(iter.Key()[2:]); err != nil {
					_ = iter.Close()

					return err
				}
				account = key.AccountKey
			}
			state := counts[account]
			if state == nil {
				state = &raftcmdpb.AccountInventoryState{}
				counts[account] = state
			}
			if code == dal.SubAttrVolume {
				if state.GetVolumeCount() == math.MaxUint32 {
					_ = iter.Close()

					return fmt.Errorf("account volume inventory overflow: %q", account.Account)
				}
				state.VolumeCount++
			} else {
				if state.GetMetadataCount() == math.MaxUint32 {
					_ = iter.Close()

					return fmt.Errorf("account metadata inventory overflow: %q", account.Account)
				}
				state.MetadataCount++
			}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()

			return err
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	staleKeys := make([][]byte, 0)
	iter, err := dal.NewBoundedIter(reader, []byte{dal.ZoneAttributes, dal.SubAttrAccountInventory}, []byte{dal.ZoneAttributes, dal.SubAttrAccountInventory + 1})
	if err != nil {
		return err
	}
	for iter.First(); iter.Valid(); iter.Next() {
		staleKeys = append(staleKeys, append([]byte(nil), iter.Key()[2:]...))
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()

		return err
	}
	if err := iter.Close(); err != nil {
		return err
	}
	readerOpen = false
	if err := reader.Close(); err != nil {
		return err
	}
	batch := store.OpenWriteSession()
	attr := attributes.New().AccountInventory
	for _, key := range staleKeys {
		if err := attr.Delete(batch, key); err != nil {
			_ = batch.Cancel()

			return err
		}
	}
	for key, state := range counts {
		if _, err := attr.Set(batch, key.Bytes(), state); err != nil {
			_ = batch.Cancel()

			return err
		}
	}
	if err := batch.Commit(); err != nil {
		return err
	}

	return nil
}
