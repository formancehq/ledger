package ctrl

import (
	"fmt"
	"math/big"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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

// scanAccount performs two forward scans — one for Volume and one for Metadata —
// and returns an assembled Account. With the type-prefixed key layout, V and M
// entries are in separate Pebble ranges.
//
// When diagLogger is non-nil, each Pebble entry is logged with its raft index
// and attribute type to help diagnose snapshot divergence issues.
func scanAccount(
	reader dal.PebbleReader,
	attrs *attributes.Attributes,
	ledgerName string,
	address string,
	schema *commonpb.MetadataSchema,
	diagLogger ...logging.Logger,
) (*commonpb.Account, error) {
	var logger logging.Logger
	if len(diagLogger) > 0 {
		logger = diagLogger[0]
	}

	// Build canonical prefix: [ledgerName padded 64B][address].
	canonicalBase := make([]byte, dal.LedgerNameFixedSize+len(address))
	copy(canonicalBase[:dal.LedgerNameFixedSize], ledgerName)
	copy(canonicalBase[dal.LedgerNameFixedSize:], address)

	// Volume scan: canonical prefix [ledger\x00][address]\x00
	volPrefix := make([]byte, len(canonicalBase)+1)
	copy(volPrefix, canonicalBase)
	volPrefix[len(canonicalBase)] = dal.CanonicalKeySepVolume

	volEntries, err := attrs.Volume.ComputeAllForPrefix(reader, volPrefix)
	if err != nil {
		return nil, fmt.Errorf("scanning volumes: %w", err)
	}

	// Metadata scan: canonical prefix [ledger\x00][address]\x01
	metaPrefix := make([]byte, len(canonicalBase)+1)
	copy(metaPrefix, canonicalBase)
	metaPrefix[len(canonicalBase)] = dal.CanonicalKeySepMetadata
	metaEntries, err := attrs.Metadata.ComputeAllForPrefix(reader, metaPrefix)
	if err != nil {
		return nil, fmt.Errorf("scanning metadata: %w", err)
	}

	if logger != nil {
		logger.WithFields(map[string]any{
			"account":     address,
			"volEntries":  len(volEntries),
			"metaEntries": len(metaEntries),
		}).Infof("scanAccount complete")
	}

	return assembleAccount(address, volEntries, metaEntries, schema), nil
}
