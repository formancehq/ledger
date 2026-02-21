package ctrl

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// assembleAccount builds a commonpb.Account from flushed volume and metadata accumulator entries.
func assembleAccount(
	address string,
	volEntries []attributes.ComputedEntry[*raftcmdpb.VolumePair],
	metaEntries []attributes.ComputedEntry[*commonpb.MetadataValue],
) *commonpb.Account {
	account := &commonpb.Account{
		Address:  address,
		Metadata: &commonpb.MetadataSet{},
	}

	if len(volEntries) > 0 {
		volumes := make(map[string]*commonpb.VolumesWithBalance, len(volEntries))
		for _, entry := range volEntries {
			var vk dal.VolumeKey
			if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
				continue
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
			volumes[vk.Asset] = &commonpb.VolumesWithBalance{
				Input:   input.String(),
				Output:  output.String(),
				Balance: new(big.Int).Sub(input, output).String(),
			}
		}
		account.Volumes = volumes
	}

	if len(metaEntries) > 0 {
		md := make(metadata.Metadata, len(metaEntries))
		for _, entry := range metaEntries {
			var mk dal.MetadataKey
			if err := mk.Unmarshal(entry.CanonicalKey); err == nil && entry.Value != nil {
				md[mk.Key] = entry.Value.Value
			}
		}
		if len(md) > 0 {
			account.Metadata = commonpb.MetadataSetFromMap(md)
		}
	}

	return account
}

// scanAccount performs a single forward scan over all Volume and Metadata attributes
// for one account and returns an assembled Account. The scan range is
// [0xF1][ledgerID][address]\x00 .. [0xF1][ledgerID][address]\x02, covering both
// volume (\x00) and metadata (\x01) canonical key separators.
func scanAccount(
	reader dal.PebbleReader,
	attrs *attributes.Attributes,
	ledgerID uint32,
	address string,
) (*commonpb.Account, error) {
	// Build bounds: [0xF1][ledgerID][address]\x00 .. [0xF1][ledgerID][address]\x02
	baseLen := 1 + 4 + len(address)
	buf := make([]byte, baseLen+1)
	buf[0] = dal.KeyPrefixAttributes
	binary.BigEndian.PutUint32(buf[1:], ledgerID)
	copy(buf[5:], address)

	lowerBound := make([]byte, baseLen+1)
	copy(lowerBound, buf)
	lowerBound[baseLen] = 0x00

	upperBound := make([]byte, baseLen+1)
	copy(upperBound, buf)
	upperBound[baseLen] = 0x02

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for account scan: %w", err)
	}
	defer func() { _ = iter.Close() }()

	volAcc := attrs.Volume.NewAccumulator()
	metaAcc := attrs.Metadata.NewAccumulator()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading value: %w", err)
		}

		attrType, ok := attributes.AttrTypeFromKey(key)
		if !ok {
			continue
		}

		switch attrType {
		case dal.AttributePrefixVolume:
			if _, err := volAcc.Feed(key, valueBytes); err != nil {
				return nil, fmt.Errorf("feeding volume: %w", err)
			}
		case dal.AttributePrefixMetadata:
			if _, err := metaAcc.Feed(key, valueBytes); err != nil {
				return nil, fmt.Errorf("feeding metadata: %w", err)
			}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return assembleAccount(address, volAcc.Flush(), metaAcc.Flush()), nil
}
