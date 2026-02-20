package ctrl

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// metadataCanonicalPrefix builds the canonical key prefix for all metadata keys
// of a given account: [ledgerID(4)][account]\x01
func metadataCanonicalPrefix(ledgerID uint32, account string) []byte {
	prefix := make([]byte, 4+len(account)+1)
	binary.BigEndian.PutUint32(prefix, ledgerID)
	copy(prefix[4:], account)
	prefix[4+len(account)] = 0x01
	return prefix
}

// volumeCanonicalPrefix builds the canonical key prefix for all volume keys
// of a given account: [ledgerID(4)][account]\x00
func volumeCanonicalPrefix(ledgerID uint32, account string) []byte {
	prefix := make([]byte, 4+len(account)+1)
	binary.BigEndian.PutUint32(prefix, ledgerID)
	copy(prefix[4:], account)
	prefix[4+len(account)] = 0x00
	return prefix
}

// GetAccountMetadata retrieves account metadata for multiple accounts from the store for a specific ledger.
// For each account, it performs a single targeted scan over that account's metadata range
// using ComputeAllForPrefix, avoiding the global attribute scan.
func GetAccountMetadata(reader data.PebbleReader, attrs *attributes.Attributes, ledgerID uint32, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata, len(accounts))

	for _, account := range accounts {
		result[account] = make(metadata.Metadata)

		prefix := metadataCanonicalPrefix(ledgerID, account)
		entries, err := attrs.Metadata.ComputeAllForPrefix(reader, ^uint64(0), prefix)
		if err != nil {
			return nil, fmt.Errorf("computing metadata for account %s: %w", account, err)
		}

		for _, entry := range entries {
			var key data.MetadataKey
			if err := key.Unmarshal(entry.CanonicalKey); err != nil {
				return nil, fmt.Errorf("parsing metadata key: %w", err)
			}
			if entry.Value != nil {
				result[account][key.Key] = entry.Value.Value
			}
		}
	}

	return result, nil
}

// GetAccountVolumes retrieves all volumes (input, output, balance) for all assets of an account.
// It performs a single targeted scan over the account's volume range using ComputeAllForPrefix,
// avoiding the global attribute scan.
func GetAccountVolumes(reader data.PebbleReader, attrs *attributes.Attributes, ledgerID uint32, account string) (map[string]*commonpb.VolumesWithBalance, error) {
	result := make(map[string]*commonpb.VolumesWithBalance)

	prefix := volumeCanonicalPrefix(ledgerID, account)
	entries, err := attrs.Volume.ComputeAllForPrefix(reader, ^uint64(0), prefix)
	if err != nil {
		return nil, fmt.Errorf("computing volumes for account %s: %w", account, err)
	}

	for _, entry := range entries {
		var key data.VolumeKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("parsing volume key: %w", err)
		}

		input := big.NewInt(0)
		output := big.NewInt(0)
		if entry.Value != nil {
			if entry.Value.InputKnown != nil {
				input = entry.Value.InputKnown.ToBigInt()
			}
			if entry.Value.OutputKnown != nil {
				output = entry.Value.OutputKnown.ToBigInt()
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
