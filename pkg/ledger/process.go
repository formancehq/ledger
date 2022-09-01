package ledger

import (
	"context"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (l *Ledger) ProcessTx(ctx context.Context, ts []core.TransactionData) (*CommitResult, error) {
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

	volumeAggregator := NewVolumeAggregator(l.store)

	generatedTxs := make([]core.ExpandedTransaction, 0)
	accounts := make(map[string]*core.Account, 0)
	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	usedReferences := make(map[string]struct{})
	for i, t := range ts {
		past := false
		if t.Timestamp.IsZero() {
			// Until v1.5.0, dates was stored as string using rfc3339 format
			// So round the date to the second to keep the same behaviour
			t.Timestamp = time.Now().UTC().Truncate(time.Second)
		} else {
			if lastTx != nil && t.Timestamp.Before(lastTx.Timestamp) {
				past = true
			}
		}
		if t.Reference != "" {
			if _, ok := usedReferences[t.Reference]; ok {
				return nil, NewConflictError()
			}
			cursor, err := l.store.GetTransactions(ctx, *NewTransactionsQuery().WithReferenceFilter(t.Reference))
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
		if past && !l.allowPastTimestamps {
			return nil, NewTransactionCommitError(i, NewValidationError("cannot pass a date prior to the last transaction"))
		}

		txVolumeAggregator := volumeAggregator.NextTx()

		for _, p := range t.Postings {
			if p.Amount.Ltz() {
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
			err := txVolumeAggregator.Transfer(ctx, p.Source, p.Destination, p.Asset, p.Amount)
			if err != nil {
				return nil, NewTransactionCommitError(i, err)
			}
		}

		for addr, volumes := range txVolumeAggregator.PostCommitVolumes() {
			for asset, volume := range volumes {
				if addr == "world" {
					continue
				}

				expectedBalance := volume.Balance()
				for _, contract := range contracts {
					if contract.Match(addr) {
						if _, ok := accounts[addr]; !ok {
							account, err := l.store.GetAccount(ctx, addr)
							if err != nil {
								return nil, err
							}
							accounts[addr] = account
						}
						if !expectedBalance.Gte(core.NewMonetaryInt(0)) {
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
			PostCommitVolumes: txVolumeAggregator.PostCommitVolumes(),
			PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes(),
		}
		lastTx = &tx
		generatedTxs = append(generatedTxs, tx)
		nextTxId++
	}

	return &CommitResult{
		PreCommitVolumes:      volumeAggregator.AggregatedPreCommitVolumes(),
		PostCommitVolumes:     volumeAggregator.AggregatedPostCommitVolumes(),
		GeneratedTransactions: generatedTxs,
	}, nil
}
