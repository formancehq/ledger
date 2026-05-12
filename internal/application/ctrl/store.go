package ctrl

import (
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
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
		Metadata: map[string]*commonpb.MetadataValue{},
	}

	if len(volEntries) > 0 {
		volumes := make(map[string]*commonpb.VolumesWithBalance, len(volEntries))
		for _, entry := range volEntries {
			var vk domain.VolumeKey

			err := vk.Unmarshal(entry.CanonicalKey)
			if err != nil {
				continue
			}

			input := big.NewInt(0)
			output := big.NewInt(0)

			if entry.Value != nil {
				if entry.Value.GetInput() != nil {
					input = entry.Value.GetInput().ToBigInt()
				}

				if entry.Value.GetOutput() != nil {
					output = entry.Value.GetOutput().ToBigInt()
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
		mdMap := make(map[string]*commonpb.MetadataValue, len(metaEntries))
		for _, entry := range metaEntries {
			var mk domain.MetadataKey

			err := mk.Unmarshal(entry.CanonicalKey)
			if err == nil && entry.Value != nil {
				mdMap[mk.Key] = entry.Value
			}
		}

		if len(mdMap) > 0 {
			account.Metadata = mdMap
			// Lazy read-path conversion: enforce schema types on stored values.
			if schema != nil {
				enforceAccountSchema(schema, mdMap)
			}
		}
	}

	return account
}

// enforceAccountSchema converts metadata values to match declared account field types.
func enforceAccountSchema(schema *commonpb.MetadataSchema, metadata map[string]*commonpb.MetadataValue) {
	if len(schema.GetAccountFields()) == 0 {
		return
	}

	for key, value := range metadata {
		fieldSchema, ok := schema.GetAccountFields()[key]
		if !ok || value == nil {
			continue
		}

		if !commonpb.TypeMatches(value, fieldSchema.GetType()) {
			metadata[key] = commonpb.ConvertMetadataValue(value, fieldSchema.GetType())
		}
	}
}

// scanAccount performs a single forward scan over all Volume and Metadata attributes
// for one account and returns an assembled Account. The scan range is
// [0xF1][ledger]\x00[address]\x00 .. [0xF1][ledger]\x00[address]\x02, covering both
// volume (\x00) and metadata (\x01) canonical key separators.
//
// When diagLogger is non-nil, each Pebble entry is logged with its raft index
// and attribute type to help diagnose snapshot divergence issues.
func scanAccount(
	reader dal.PebbleReader,
	attrs *attributes.Attributes,
	ledger string,
	address string,
	schema *commonpb.MetadataSchema,
	diagLogger ...logging.Logger,
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
	lowerBound[baseLen] = dal.CanonicalKeySepVolume

	upperBound := make([]byte, baseLen+1)
	copy(upperBound, buf)
	upperBound[baseLen] = dal.CanonicalKeySepMetadata + 1

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

	var logger logging.Logger
	if len(diagLogger) > 0 {
		logger = diagLogger[0]
	}

	entryCount := 0

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

		entryCount++

		if logger != nil {
			canonical := string(key[1 : len(key)-attributes.SuffixLen])
			logger.WithFields(map[string]any{
				"account":      address,
				"attrType":     fmt.Sprintf("0x%02x", attrType),
				"canonicalKey": canonical,
				"valueLen":     len(valueBytes),
			}).Infof("scanAccount entry")
		}

		switch attrType {
		case dal.AttributeCodeVolume:
			if _, err := volAcc.Feed(key, valueBytes); err != nil {
				return nil, fmt.Errorf("feeding volume: %w", err)
			}
		case dal.AttributeCodeMetadata:
			if _, err := metaAcc.Feed(key, valueBytes); err != nil {
				return nil, fmt.Errorf("feeding metadata: %w", err)
			}
		}
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	volEntries := volAcc.Flush()
	metaEntries := metaAcc.Flush()

	if logger != nil {
		logger.WithFields(map[string]any{
			"account":      address,
			"totalEntries": entryCount,
			"volEntries":   len(volEntries),
			"metaEntries":  len(metaEntries),
		}).Infof("scanAccount complete")
	}

	return assembleAccount(address, volEntries, metaEntries, schema), nil
}
