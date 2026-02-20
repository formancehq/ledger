package ctrl

import (
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// GetAccountMetadata retrieves account metadata for multiple accounts from the store for a specific ledger.
// It uses the attributes system to list all metadata keys and compute their values.
func GetAccountMetadata(reader data.PebbleReader, attrs *attributes.Attributes, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	// Initialize with empty metadata for all requested accounts
	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	// Create a set of requested accounts for fast lookup
	accountSet := make(map[string]struct{}, len(accounts))
	for _, account := range accounts {
		accountSet[account] = struct{}{}
	}

	// List all metadata keys
	entries, err := attrs.Metadata.List(reader)
	if err != nil {
		return nil, fmt.Errorf("listing metadata keys: %w", err)
	}

	// Filter entries by ledger and account, then compute values
	for _, entry := range entries {
		// Parse the canonical key
		var key data.MetadataKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("parsing metadata key: %w", err)
		}

		// Skip if not the requested ledger
		if key.LedgerID != ledgerID {
			continue
		}

		// Skip if not one of the requested accounts
		if _, ok := accountSet[key.Account]; !ok {
			continue
		}

		// Compute the metadata value (use max uint64 to get the latest value)
		value, err := attrs.Metadata.ComputeValue(reader, ^uint64(0), entry.CanonicalKey)
		if err != nil {
			return nil, fmt.Errorf("computing metadata value for %d/%s/%s: %w",
				key.LedgerID, key.Account, key.Key, err)
		}

		// Skip nil values (deleted metadata)
		if value != nil {
			result[key.Account][key.Key] = value.Value
		}
	}

	return result, nil
}

// GetAccountVolumes retrieves all volumes (input, output, balance) for all assets of an account.
// It uses the attributes system to list all volume keys and compute cumulative values.
func GetAccountVolumes(reader data.PebbleReader, attrs *attributes.Attributes, ledgerID uint32, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	result := make(map[string]*commonpb.VolumesWithBalance)
	const maxIndex uint64 = 1 << 62

	// List all Volume entries
	volumeEntries, err := attrs.Volume.List(reader)
	if err != nil {
		return nil, fmt.Errorf("listing volume entries: %w", err)
	}

	// Filter entries by ledger and account, then compute values
	for _, entry := range volumeEntries {
		var key data.VolumeKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("parsing volume key: %w", err)
		}
		if key.LedgerID != ledgerID || key.Account != account {
			continue
		}

		// Compute the merged VolumePair for this asset
		pair, err := attrs.Volume.ComputeValue(reader, maxIndex, entry.CanonicalKey)
		if err != nil {
			return nil, fmt.Errorf("computing volume for %s: %w", key.Asset, err)
		}

		input := big.NewInt(0)
		output := big.NewInt(0)
		if pair != nil {
			if pair.InputKnown != nil {
				input = pair.InputKnown.ToBigInt()
			}
			if pair.OutputKnown != nil {
				output = pair.OutputKnown.ToBigInt()
			}
		}
		balance := new(big.Int).Sub(input, output)

		result[key.Asset] = &commonpb.VolumesWithBalance{
			Input:   input.String(),
			Output:  output.String(),
			Balance: balance.String(),
		}
	}

	return result, nil
}
