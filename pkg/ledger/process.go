package ledger

import (
	"context"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (l *Ledger) processTx(ctx context.Context, ts []core.TransactionData) (*core.CommitResult, error) {
	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "loading mapping")
	}

	var nextTxId uint64
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return nil, err
	}
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	volumeAggregator := newVolumeAggregator(l.store)

	generatedTxs := make([]core.Transaction, 0)
	accounts := make(map[string]*core.Account, 0)
	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	for i, t := range ts {
		if len(t.Postings) == 0 {
			return nil, NewTransactionCommitError(i, NewValidationError("transaction has no postings"))
		}

		txVolumeAggregator := volumeAggregator.nextTx()

		for _, p := range t.Postings {
			if p.Amount < 0 {
				return nil, NewTransactionCommitError(i, NewValidationError("negative amount"))
			}
			if !core.ValidateAddress(p.Source) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid source address"))
			}
			if !core.ValidateAddress(p.Destination) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid destination address"))
			}
			if !core.AssetIsValid(p.Asset) {
				return nil, NewTransactionCommitError(i, NewValidationError("invalid asset"))
			}
			err := txVolumeAggregator.transfer(ctx, p.Source, p.Destination, p.Asset, uint64(p.Amount))
			if err != nil {
				return nil, NewTransactionCommitError(i, err)
			}
		}

		for addr, volumes := range txVolumeAggregator.postCommitVolumes() {
			for asset, volume := range volumes {
				if addr == "world" {
					continue
				}

				expectedBalance := volume.Balance()
				for _, contract := range contracts {
					if contract.Match(addr) {
						account, ok := accounts[addr]
						if !ok {
							account, err = l.store.GetAccount(ctx, addr)
							if err != nil {
								return nil, err
							}
							accounts[addr] = account
						}

						if ok = contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": float64(expectedBalance),
							},
							Metadata: account.Metadata,
							Asset:    asset,
						}); !ok {
							return nil, NewTransactionCommitError(i, NewInsufficientFundError(asset))
						}
						break
					}
				}
			}
		}

		tx := core.Transaction{
			TransactionData:   t,
			ID:                nextTxId,
			Timestamp:         time.Now().UTC().Format(time.RFC3339),
			PostCommitVolumes: txVolumeAggregator.postCommitVolumes(),
			PreCommitVolumes:  txVolumeAggregator.preCommitVolumes(),
		}
		generatedTxs = append(generatedTxs, tx)
		nextTxId++
	}

	return &core.CommitResult{
		PreCommitVolumes:      volumeAggregator.aggregatedPreCommitVolumes(),
		PostCommitVolumes:     volumeAggregator.aggregatedPostCommitVolumes(),
		GeneratedTransactions: generatedTxs,
	}, nil
}
