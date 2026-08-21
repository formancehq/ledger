package query

import (
	"context"
	"errors"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

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

	volumes, err := view.ReadVolumesSelected(
		ledgerName,
		temporality,
		at,
		accounts,
		accountPrefixes,
	)
	if err != nil {
		return nil, err
	}

	return aggregateHistoricalVolumeRows(ctx, volumes, match, opts)
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
