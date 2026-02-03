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
func GetAccountMetadata(store *data.Store, attrs *attributes.Attributes, ledgerName string, accounts []string) (map[string]metadata.Metadata, error) {
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
	entries, err := attrs.Metadata.List(store)
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
		if key.LedgerName != ledgerName {
			continue
		}

		// Skip if not one of the requested accounts
		if _, ok := accountSet[key.Account]; !ok {
			continue
		}

		// Compute the metadata value (use max uint64 to get the latest value)
		value, err := attrs.Metadata.ComputeValue(store, ^uint64(0), entry.Hash128)
		if err != nil {
			return nil, fmt.Errorf("computing metadata value for %s/%s/%s: %w",
				key.LedgerName, key.Account, key.Key, err)
		}

		// Skip nil values (deleted metadata)
		if value != nil {
			result[key.Account][key.Key] = value.Value
		}
	}

	return result, nil
}

// assetEntry holds a VolumeKey and its corresponding U128 hash for volume computation.
type assetEntry struct {
	key data.VolumeKey
	id  attributes.U128
}

// GetAccountVolumes retrieves all volumes (input, output, balance) for all assets of an account.
// It uses the attributes system to list all keys and compute cumulative values.
func GetAccountVolumes(s *data.Store, attrs *attributes.Attributes, ledgerName string, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	result := make(map[string]*commonpb.VolumesWithBalance)
	const maxIndex uint64 = 1 << 62

	// Collect all assets from both Input and Output mapping tables
	// Store both the asset name and the U128 hash for later computation
	assetEntries := make(map[string]assetEntry)

	// List all Input entries
	inputEntries, err := attrs.Input.List(s)
	if err != nil {
		return nil, fmt.Errorf("listing input entries: %w", err)
	}
	for _, entry := range inputEntries {
		var key data.VolumeKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("parsing input key: %w", err)
		}
		if key.LedgerName == ledgerName && key.Account == account {
			assetEntries[key.Asset] = assetEntry{key: key, id: entry.Hash128}
		}
	}

	// List all Output entries
	outputEntries, err := attrs.Output.List(s)
	if err != nil {
		return nil, fmt.Errorf("listing output entries: %w", err)
	}
	for _, entry := range outputEntries {
		var key data.VolumeKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("parsing output key: %w", err)
		}
		if key.LedgerName == ledgerName && key.Account == account {
			// Only add if not already present from input entries
			if _, exists := assetEntries[key.Asset]; !exists {
				assetEntries[key.Asset] = assetEntry{key: key, id: entry.Hash128}
			}
		}
	}

	// For each asset, compute Input and Output values
	for asset, entry := range assetEntries {
		// Use the stored hash ID directly (already computed and stored by Touch)
		inputValue, err := attrs.Input.ComputeValue(s, maxIndex, entry.id)
		if err != nil {
			return nil, fmt.Errorf("computing input for %s: %w", asset, err)
		}

		// Use the same ID for Output (same canonical key, different attribute prefix)
		outputValue, err := attrs.Output.ComputeValue(s, maxIndex, entry.id)
		if err != nil {
			return nil, fmt.Errorf("computing output for %s: %w", asset, err)
		}

		input := inputValue.Value()
		output := outputValue.Value()
		balance := new(big.Int).Sub(input, output)

		result[asset] = &commonpb.VolumesWithBalance{
			Input:   input.String(),
			Output:  output.String(),
			Balance: balance.String(),
		}
	}

	return result, nil
}
