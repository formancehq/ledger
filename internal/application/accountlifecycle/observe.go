package accountlifecycle

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func ObserveAccount(reader dal.PebbleGetter, attrs *attributes.Attributes, coverage *plan.Coverage, account domain.AccountKey) (*raftcmdpb.AccountInventoryObservation, error) {
	var volumes, metadata uint32
	for code, entries := range coverage.Attributes {
		if code != dal.SubAttrVolume && code != dal.SubAttrMetadata {
			continue
		}
		for id, entry := range entries {
			if !coverage.IsPersisted(code, id) {
				continue
			}
			var owner domain.AccountKey
			if code == dal.SubAttrVolume {
				var key domain.VolumeKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return nil, err
				}
				owner = key.AccountKey
			} else {
				var key domain.MetadataKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return nil, err
				}
				owner = key.AccountKey
			}
			if owner != account {
				continue
			}
			if code == dal.SubAttrVolume {
				volumes++
			} else {
				metadata++
			}
		}
	}
	expected, err := attrs.AccountInventory.Get(reader, account.Bytes())
	if err != nil {
		return nil, err
	}
	if expected == nil {
		if volumes != 0 || metadata != 0 {
			return nil, fmt.Errorf("account inventory missing for %q", account.Account)
		}
	} else if expected.GetVolumeCount() != volumes || expected.GetMetadataCount() != metadata {
		return nil, fmt.Errorf("account inventory mismatch for %q: summary=(%d,%d) scanned=(%d,%d)", account.Account, expected.GetVolumeCount(), expected.GetMetadataCount(), volumes, metadata)
	}
	ledgerInfo, err := attrs.Ledger.Get(reader, domain.LedgerKey{Name: account.LedgerName}.Bytes())
	if err != nil {
		return nil, err
	}
	digest, err := accounttype.LedgerTypeDigest(ledgerInfo)
	if err != nil {
		return nil, err
	}

	return &raftcmdpb.AccountInventoryObservation{AccountKey: account.Bytes(), Expected: expected, AccountTypesDigest: digest}, nil
}
