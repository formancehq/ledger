package query

import (
	"context"
	"errors"
	"math/big"
	"sort"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

// AggregateHistoricalVolumes applies the existing AggregateVolumes result
// semantics to a pinned history view. accounts has three states:
//   - nil: unfiltered (all historical account rows for the ledger are folded)
//   - empty non-nil: a current filter matched no accounts
//   - non-empty: exact current account selection
func AggregateHistoricalVolumes(
	view *balancehistorystore.View,
	ledgerName string,
	temporality balancehistorystore.Temporality,
	at uint64,
	accounts []string,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	return aggregateHistoricalVolumes(context.Background(), view, ledgerName, temporality, at, accounts, nil, opts)
}

// AggregateHistoricalVolumesMatching applies an account predicate to the
// identities stored in a pinned history view. It is used by historical address
// filters, whose universe includes accounts that no longer exist in the
// current read store. When accounts is non-nil, the store first restricts the
// scan to those exact current accounts; match is then applied as an additional
// condition.
//
// Cancellation is checked before and after the store lookup and while folding
// rows. The pinned view remains responsible for closing its Pebble snapshot.
func AggregateHistoricalVolumesMatching(
	ctx context.Context,
	view *balancehistorystore.View,
	ledgerName string,
	temporality balancehistorystore.Temporality,
	at uint64,
	accounts []string,
	match func(account string) bool,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	return AggregateHistoricalVolumesSelected(ctx, view, ledgerName, temporality, at, accounts, nil, match, opts)
}

// AggregateHistoricalVolumesSelected reads the union of exact account and
// account-prefix candidates before applying match. It lets a mixed temporal
// filter such as `address startsWith X OR current metadata matches Y` avoid a
// full historical-ledger scan while preserving its final boolean predicate.
func AggregateHistoricalVolumesSelected(
	ctx context.Context,
	view *balancehistorystore.View,
	ledgerName string,
	temporality balancehistorystore.Temporality,
	at uint64,
	accounts []string,
	accountPrefixes []string,
	match func(account string) bool,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	if len(accountPrefixes) == 0 {
		return aggregateHistoricalVolumes(ctx, view, ledgerName, temporality, at, accounts, match, opts)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if view == nil {
		return nil, errors.New("balance history view is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	volumes := make([]balancehistorystore.Volume, 0)
	seen := make(map[historicalVolumeIdentity]struct{})
	appendUnique := func(rows []balancehistorystore.Volume) {
		for _, row := range rows {
			identity := historicalVolumeIdentity{
				account:   row.Account,
				assetBase: row.AssetBase,
				precision: row.AssetPrecision,
				color:     row.Color,
			}
			if _, ok := seen[identity]; ok {
				continue
			}
			seen[identity] = struct{}{}
			volumes = append(volumes, row)
		}
	}

	if accounts != nil {
		rows, err := view.ReadVolumes(ledgerName, temporality, at, accounts)
		if err != nil {
			return nil, err
		}
		appendUnique(rows)
	}
	for _, prefix := range deduplicateStrings(accountPrefixes) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := view.ReadVolumesByPrefix(ledgerName, temporality, at, prefix)
		if err != nil {
			return nil, err
		}
		appendUnique(rows)
	}

	sort.Slice(volumes, func(i, j int) bool {
		left, right := volumes[i], volumes[j]
		if left.Account != right.Account {
			return left.Account < right.Account
		}
		if left.AssetBase != right.AssetBase {
			return left.AssetBase < right.AssetBase
		}
		if left.AssetPrecision != right.AssetPrecision {
			return left.AssetPrecision < right.AssetPrecision
		}

		return left.Color < right.Color
	})

	return aggregateHistoricalVolumeRows(ctx, volumes, match, opts)
}

// AggregateHistoricalVolumesByPrefix uses the history store's prefix-seekable
// volume catalog. Unlike evaluating a prefix predicate after ReadVolumes(nil),
// its I/O is proportional to the matching historical account range rather
// than to every historical identity in the ledger.
func AggregateHistoricalVolumesByPrefix(
	ctx context.Context,
	view *balancehistorystore.View,
	ledgerName string,
	temporality balancehistorystore.Temporality,
	at uint64,
	accountPrefix string,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	return AggregateHistoricalVolumesSelected(ctx, view, ledgerName, temporality, at, nil, []string{accountPrefix}, nil, opts)
}

type historicalVolumeIdentity struct {
	account   string
	assetBase string
	precision uint8
	color     string
}

func deduplicateStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}

	return unique
}

func aggregateHistoricalVolumes(
	ctx context.Context,
	view *balancehistorystore.View,
	ledgerName string,
	temporality balancehistorystore.Temporality,
	at uint64,
	accounts []string,
	match func(account string) bool,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	if view == nil {
		return nil, errors.New("balance history view is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	volumes, err := view.ReadVolumes(ledgerName, temporality, at, accounts)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return aggregateHistoricalVolumeRows(ctx, volumes, match, opts)
}

func aggregateHistoricalVolumeRows(
	ctx context.Context,
	volumes []balancehistorystore.Volume,
	match func(account string) bool,
	opts AggregateOptions,
) (*commonpb.AggregateResult, error) {
	accumulator := newAccumulator(opts)
	var (
		previousAccount string
		accountMatches  bool
		haveAccount     bool
	)
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if match != nil {
			if !haveAccount || volume.Account != previousAccount {
				previousAccount = volume.Account
				accountMatches = match(volume.Account)
				haveAccount = true
			}
			if !accountMatches {
				continue
			}
		}

		pair, err := historicalPair(volume.Input, volume.Output)
		if err != nil {
			return nil, err
		}
		key := domain.VolumeKey{
			AccountKey:     domain.AccountKey{Account: volume.Account},
			Asset:          domain.FormatAsset(volume.AssetBase, volume.AssetPrecision),
			AssetBase:      volume.AssetBase,
			AssetPrecision: volume.AssetPrecision,
			Color:          volume.Color,
		}
		if err := accumulator.accumulate(attributes.ComputedEntry[*raftcmdpb.VolumePair]{
			CanonicalKey: key.Bytes(),
			Value:        pair,
		}); err != nil {
			return nil, err
		}
	}

	return accumulator.result()
}

func historicalPair(input, output *big.Int) (*raftcmdpb.VolumePair, error) {
	inputProto, err := historicalUint256(input, "input")
	if err != nil {
		return nil, err
	}
	outputProto, err := historicalUint256(output, "output")
	if err != nil {
		return nil, err
	}

	return &raftcmdpb.VolumePair{Input: inputProto, Output: outputProto}, nil
}

func historicalUint256(value *big.Int, side string) (*commonpb.Uint256, error) {
	if value == nil {
		return commonpb.NewUint256FromUint64(0), nil
	}
	if value.Sign() < 0 || value.BitLen() > 256 {
		return nil, &ErrAggregateOverflow{Stage: "history-value", Side: side}
	}

	var converted uint256.Int
	converted.SetBytes(value.Bytes())

	return commonpb.NewUint256(&converted), nil
}
