package ctrl

import (
	"fmt"
	"math/big"
	"sort"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// assembleAccount builds a commonpb.Account from flushed volume and metadata accumulator entries.
// When collapseColors is true, all colored buckets of the same asset are summed
// into a single entry with Color = "" in the returned Account.volumes list.
func assembleAccount(
	address string,
	volEntries []attributes.ComputedEntry[*raftcmdpb.VolumePair],
	metaEntries []attributes.ComputedEntry[*commonpb.MetadataValue],
	collapseColors bool,
) (*commonpb.Account, error) {
	account := &commonpb.Account{
		Address:  address,
		Metadata: map[string]*commonpb.MetadataValue{},
	}

	if len(volEntries) > 0 {
		vols, err := buildAccountVolumes(volEntries, collapseColors)
		if err != nil {
			return nil, err
		}

		account.Volumes = vols
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
		}
	}

	return account, nil
}

// buildAccountVolumes turns the flushed volume entries into the
// `repeated AccountVolume` list carried by Account.volumes. The list is
// sorted by (asset, color) ascending. If collapseColors is true, entries
// with the same asset (different colors) are summed under color = "".
//
// A malformed canonical key surfaces a hard error rather than a silent
// `continue`: every other Pebble scan path in the codebase propagates
// unmarshal errors, and silently dropping a row from GetAccount would
// return a truncated balance the caller has no way to detect (CLAUDE.md
// invariant #7).
func buildAccountVolumes(volEntries []attributes.ComputedEntry[*raftcmdpb.VolumePair], collapseColors bool) ([]*commonpb.AccountVolume, error) {
	type key struct {
		asset string
		color string
	}

	// running holds the accumulating sums as *big.Int so we never round-trip
	// through decimal strings per row; the final AccountVolume (formatted once)
	// is built at the end.
	type running struct {
		asset  string
		color  string
		input  *big.Int
		output *big.Int
	}

	totals := make(map[key]*running, len(volEntries))

	for _, entry := range volEntries {
		var vk domain.VolumeKey
		if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("malformed volume canonical key in account scan: %w", err)
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

		bucketColor := vk.Color
		if collapseColors {
			bucketColor = ""
		}

		k := key{asset: vk.Asset, color: bucketColor}
		acc, ok := totals[k]
		if !ok {
			acc = &running{
				asset:  vk.Asset,
				color:  bucketColor,
				input:  big.NewInt(0),
				output: big.NewInt(0),
			}
			totals[k] = acc
		}

		acc.input.Add(acc.input, input)
		acc.output.Add(acc.output, output)
	}

	out := make([]*commonpb.AccountVolume, 0, len(totals))
	for _, v := range totals {
		// Format the accumulated sums once, and compute the balance from the
		// *big.Int totals (after any color collapse).
		out = append(out, &commonpb.AccountVolume{
			Asset: v.asset,
			Color: v.color,
			Volumes: &commonpb.VolumesWithBalance{
				Input:   v.input.String(),
				Output:  v.output.String(),
				Balance: new(big.Int).Sub(v.input, v.output).String(),
			},
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return commonpb.LessByAssetColor(out[i], out[j])
	})

	return out, nil
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
	collapseColors bool,
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

	return assembleAccount(address, volEntries, metaEntries, collapseColors)
}
