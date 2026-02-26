package ctrl

import (
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// assembleAccount builds a commonpb.Account from flushed volume and metadata accumulator entries.
// If schema is non-nil, stored values are lazily converted to match declared types.
func assembleAccount(
	address string,
	volEntries []attributes.ComputedEntry[*raftcmdpb.VolumePair],
	metaEntries []attributes.ComputedEntry[*commonpb.MetadataValue],
	schema *commonpb.MetadataSchema,
) *commonpb.Account {
	account := &commonpb.Account{
		Address:  address,
		Metadata: &commonpb.MetadataSet{},
	}

	if len(volEntries) > 0 {
		volumes := make(map[string]*commonpb.VolumesWithBalance, len(volEntries))
		for _, entry := range volEntries {
			var vk domain.VolumeKey
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
		mdList := make([]*commonpb.Metadata, 0, len(metaEntries))
		for _, entry := range metaEntries {
			var mk domain.MetadataKey
			if err := mk.Unmarshal(entry.CanonicalKey); err == nil && entry.Value != nil {
				mdList = append(mdList, &commonpb.Metadata{
					Key:   mk.Key,
					Value: entry.Value,
				})
			}
		}
		if len(mdList) > 0 {
			account.Metadata = &commonpb.MetadataSet{Metadata: mdList}
			// Lazy read-path conversion: enforce schema types on stored values.
			if schema != nil {
				enforceAccountSchema(schema, mdList)
			}
		}
	}

	return account
}

// enforceAccountSchema converts metadata values to match declared account field types.
func enforceAccountSchema(schema *commonpb.MetadataSchema, metadata []*commonpb.Metadata) {
	if len(schema.AccountFields) == 0 {
		return
	}
	for _, m := range metadata {
		fieldSchema, ok := schema.AccountFields[m.Key]
		if !ok || m.Value == nil {
			continue
		}
		if !commonpb.TypeMatches(m.Value, fieldSchema.Type) {
			m.Value = commonpb.ConvertMetadataValue(m.Value, fieldSchema.Type)
		}
	}
}

// scanAccount performs a single forward scan over all Volume and Metadata attributes
// for one account and returns an assembled Account. The scan range is
// [0xF1][ledger]\x00[address]\x00 .. [0xF1][ledger]\x00[address]\x02, covering both
// volume (\x00) and metadata (\x01) canonical key separators.
func scanAccount(
	reader dal.PebbleReader,
	attrs *attributes.Attributes,
	ledger string,
	address string,
	schema *commonpb.MetadataSchema,
) (*commonpb.Account, error) {
	// Build bounds: [0xF1][ledger]\x00[address]\x00 .. [0xF1][ledger]\x00[address]\x02
	baseLen := 1 + len(ledger) + 1 + len(address)
	buf := make([]byte, baseLen+1)
	buf[0] = dal.KeyPrefixAttributes
	n := 1
	n += copy(buf[n:], ledger)
	buf[n] = 0x00
	n++
	copy(buf[n:], address)

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

	return assembleAccount(address, volAcc.Flush(), metaAcc.Flush(), schema), nil
}
