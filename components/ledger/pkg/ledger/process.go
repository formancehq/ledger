package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (l *Ledger) ProcessTxsData(ctx context.Context, txsData ...core.TransactionData) (CommitResult, error) {
	var nextTxId uint64
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return CommitResult{}, err
	}
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	volumeAggregator := NewVolumeAggregator(l.store)

	generatedTxs := make([]core.ExpandedTransaction, 0)

	usedReferences := make(map[string]struct{})
	for i, t := range txsData {
		if t.Timestamp.IsZero() {
			// Until v1.5.0, dates was stored as string using rfc3339 format
			// So round the date to the second to keep the same behaviour
			t.Timestamp = time.Now().UTC().Truncate(time.Second)
		}

		if t.Reference != "" {
			if _, ok := usedReferences[t.Reference]; ok {
				return CommitResult{}, NewConflictError()
			}
			cursor, err := l.store.GetTransactions(ctx, *NewTransactionsQuery().WithReferenceFilter(t.Reference))
			if err != nil {
				return CommitResult{}, err
			}
			if len(cursor.Data) > 0 {
				return CommitResult{}, NewConflictError()
			}
			usedReferences[t.Reference] = struct{}{}
		}

		txVolumeAggregator := volumeAggregator.NextTx()

		for _, p := range t.Postings {
			if err := txVolumeAggregator.Transfer(ctx, p.Source, p.Destination, p.Asset, p.Amount); err != nil {
				return CommitResult{}, NewTransactionCommitError(i, err)
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

	return CommitResult{
		PreCommitVolumes:      volumeAggregator.AggregatedPreCommitVolumes(),
		PostCommitVolumes:     volumeAggregator.AggregatedPostCommitVolumes(),
		GeneratedTransactions: generatedTxs,
	}, nil
}

func (l *Ledger) ValidatePostings(ctx context.Context, txsData ...core.TransactionData) (int, error) {
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "GetLastTransaction")
	}

	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "loading mapping")
	}
	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	volumeAggregator := NewVolumeAggregator(l.store)

	for i, t := range txsData {
		past := false
		if !t.Timestamp.IsZero() {
			if lastTx != nil && t.Timestamp.Before(lastTx.Timestamp) {
				past = true
			}
		}
		if len(t.Postings) == 0 {
			return i, NewValidationError("transaction has no postings")
		}
		if past && !l.allowPastTimestamps {
			return i, NewValidationError("cannot pass a date prior to the last transaction")
		}

		txVolumeAggregator := volumeAggregator.NextTx()

		for _, p := range t.Postings {
			if p.Amount.Ltz() {
				return i, NewValidationError("negative amount")
			}
			if !core.ValidateAddress(p.Source) {
				return i, NewValidationError("invalid source address")
			}
			if !core.ValidateAddress(p.Destination) {
				return i, NewValidationError("invalid destination address")
			}
			if !core.AssetIsValid(p.Asset) {
				return i, NewValidationError("invalid asset")
			}
			if err := txVolumeAggregator.Transfer(ctx, p.Source, p.Destination, p.Asset, p.Amount); err != nil {
				return i, errors.Wrap(err, "transferring volumes")
			}
		}

		accounts := make(map[string]*core.Account, 0)
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
								return i, errors.Wrap(err, fmt.Sprintf("GetAccount '%s'", addr))
							}
							accounts[addr] = account
						}
						if ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": expectedBalance,
							},
							Metadata: accounts[addr].Metadata,
							Asset:    asset,
						}); !ok {
							return i, NewInsufficientFundError(asset)
						}
						break
					}
				}
			}
		}
	}

	return 0, nil
}
