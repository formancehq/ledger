package ledger

import (
	"context"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (l *Ledger) processTx(ctx context.Context, ts []core.TransactionData) (*CommitResult, error) {
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

	generatedTxs := make([]core.ExpandedTransaction, 0)
	accounts := make(map[string]*core.Account, 0)
	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	usedReferences := make(map[string]struct{})
	for i, t := range ts {
		if t.Timestamp.IsZero() {
			// Until v1.5.0, dates was stored as string using rfc3339 format
			// So round the date to the second to keep the same behaviour
			t.Timestamp = time.Now().UTC().Truncate(time.Second)
		}
		if t.Reference != "" {
			if _, ok := usedReferences[t.Reference]; ok {
				return nil, NewConflictError()
			}
			cursor, err := l.store.GetTransactions(ctx, *storage.NewTransactionsQuery().WithReferenceFilter(t.Reference))
			if err != nil {
				return nil, err
			}
			if len(cursor.Data) > 0 {
				return nil, NewConflictError()
			}
			usedReferences[t.Reference] = struct{}{}
		}
		if len(t.Postings) == 0 {
			return nil, NewTransactionCommitError(i, NewValidationError("transaction has no postings"))
		}
		if lastTx != nil && t.Timestamp.Before(lastTx.Timestamp) {
			return nil, NewTransactionCommitError(i, NewValidationError("cannot pass a date prior to the last transaction"))
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

		tx := core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: t,
				ID:              nextTxId,
			},
			PostCommitVolumes: txVolumeAggregator.postCommitVolumes(),
			PreCommitVolumes:  txVolumeAggregator.preCommitVolumes(),
		}
		lastTx = &tx
		generatedTxs = append(generatedTxs, tx)
		nextTxId++
	}

	return &CommitResult{
		PreCommitVolumes:      volumeAggregator.aggregatedPreCommitVolumes(),
		PostCommitVolumes:     volumeAggregator.aggregatedPostCommitVolumes(),
		GeneratedTransactions: generatedTxs,
	}, nil
}
