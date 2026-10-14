package accountlifecycle

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func Accounts(coverage *plan.Coverage) (map[domain.AccountKey]struct{}, error) {
	accounts := make(map[domain.AccountKey]struct{})
	for attrCode, entries := range coverage.Attributes {
		for _, entry := range entries {
			switch attrCode {
			case dal.SubAttrVolume:
				var key domain.VolumeKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return nil, err
				}
				accounts[key.AccountKey] = struct{}{}
			case dal.SubAttrMetadata:
				var key domain.MetadataKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return nil, err
				}
				accounts[key.AccountKey] = struct{}{}
			}
		}
	}

	return accounts, nil
}

func AccountsForOrders(perOrder []*plan.Coverage) (map[domain.AccountKey]struct{}, error) {
	out := make(map[domain.AccountKey]struct{})
	for _, coverage := range perOrder {
		accounts, err := Accounts(coverage)
		if err != nil {
			return nil, err
		}
		for account := range accounts {
			out[account] = struct{}{}
		}
	}

	return out, nil
}

// MarkAccountCandidateRows retains canonical bytes for touched keys that do
// not yet exist in Pebble. The FSM needs those keys to evaluate final liveness.
func MarkAccountCandidateRows(account domain.AccountKey, coverage, aggregate *plan.Coverage) error {
	for code, entries := range coverage.Attributes {
		if code != dal.SubAttrVolume && code != dal.SubAttrMetadata {
			continue
		}
		for _, entry := range entries {
			var owner domain.AccountKey
			if code == dal.SubAttrVolume {
				var key domain.VolumeKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return err
				}
				owner = key.AccountKey
			} else {
				var key domain.MetadataKey
				if err := key.Unmarshal(entry.Canonical); err != nil {
					return err
				}
				owner = key.AccountKey
			}
			if owner == account {
				coverage.AddCandidate(code, entry.Canonical)
				aggregate.AddCandidate(code, append([]byte(nil), entry.Canonical...))
			}
		}
	}

	return nil
}

func AddPersistedRows(reader dal.PebbleReader, account domain.AccountKey, coverage, aggregate *plan.Coverage) error {
	for _, spec := range []struct{ attrCode, separator byte }{
		{dal.SubAttrVolume, dal.CanonicalKeySepVolume},
		{dal.SubAttrMetadata, dal.CanonicalKeySepMetadata},
	} {
		canonicalPrefix := append(domain.LedgerScopedPrefix(account.LedgerName), account.Account...)
		canonicalPrefix = append(canonicalPrefix, spec.separator)
		lower := append([]byte{dal.ZoneAttributes, spec.attrCode}, canonicalPrefix...)
		upper := append([]byte(nil), lower...)
		upper[len(upper)-1]++
		iter, err := dal.NewBoundedIter(reader, lower, upper)
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			canonical := append([]byte(nil), iter.Key()[2:]...)
			coverage.AddLifecycleCandidate(spec.attrCode, canonical)
			aggregate.AddLifecycleCandidate(spec.attrCode, append([]byte(nil), canonical...))
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()

			return err
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}

	return nil
}
