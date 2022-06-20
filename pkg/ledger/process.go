package ledger

import (
	"context"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

type volumeAggregator struct {
	aggregatePreCommitVol  core.AccountsAssetsVolumes
	aggregatePostCommitVol core.AccountsAssetsVolumes

	txPreCommitVol  core.AccountsAssetsVolumes
	txPostCommitVol core.AccountsAssetsVolumes
}

func (l *Ledger) processTx(ctx context.Context, txsData []core.TransactionData) (*CommitResult, error) {
	lastLog, err := l.store.LastLog(ctx)
	if err != nil {
		return nil, err
	}

	var nextTxId uint64
	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return nil, err
	}
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
	}

	generatedTxs := make([]core.Transaction, len(txsData))

	a := volumeAggregator{
		aggregatePreCommitVol:  core.AccountsAssetsVolumes{},
		aggregatePostCommitVol: core.AccountsAssetsVolumes{},

		txPreCommitVol:  core.AccountsAssetsVolumes{},
		txPostCommitVol: core.AccountsAssetsVolumes{},
	}

	generatedLogs := make([]core.Log, len(txsData))

	contracts := make([]core.Contract, 0)
	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "loading mapping")
	}
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	accMatchingContracts := make(map[string]core.Account, 0)

	for i, txData := range txsData {
		if len(txData.Postings) == 0 {
			return nil, NewTransactionCommitError(i, NewValidationError("transaction has no postings"))
		}

		a.txPreCommitVol = core.AccountsAssetsVolumes{}
		a.txPostCommitVol = core.AccountsAssetsVolumes{}

		for _, p := range txData.Postings {
			if err := p.Validate(); err != nil {
				return nil, NewTransactionCommitError(i, NewValidationError(err.Error()))
			}

			initCommitVolumes(a, p)

			if err := l.updateVolumes(p.Source, 0, p.Amount,
				ctx, a, p, generatedTxs); err != nil {
				return nil, NewTransactionCommitError(i, err)
			}
			if err := l.updateVolumes(p.Destination, p.Amount, 0,
				ctx, a, p, generatedTxs); err != nil {
				return nil, NewTransactionCommitError(i, err)
			}
		}

		if err := l.checkPostCommitVolumes(ctx, a.txPostCommitVol, contracts, accMatchingContracts); err != nil {
			return nil, NewTransactionCommitError(i, err)
		}

		generatedTxs[i] = core.Transaction{
			TransactionData:   txData,
			ID:                nextTxId,
			Timestamp:         time.Now().UTC().Format(time.RFC3339),
			PreCommitVolumes:  a.txPreCommitVol,
			PostCommitVolumes: a.txPostCommitVol,
		}
		generatedLogs[i] = core.NewTransactionLog(lastLog, generatedTxs[i])
		lastLog = &generatedLogs[i]
		nextTxId++
	}

	return &CommitResult{
		PreCommitVolumes:      a.aggregatePreCommitVol,
		PostCommitVolumes:     a.aggregatePostCommitVol,
		GeneratedTransactions: generatedTxs,
		GeneratedLogs:         generatedLogs,
	}, nil
}

func (l *Ledger) updateVolumes(account string, inputAmount, outputAmount int64,
	ctx context.Context, a volumeAggregator,
	p core.Posting, generatedTxs []core.Transaction) error {
	if _, ok := a.txPreCommitVol[account][p.Asset]; !ok {
		for _, tx := range generatedTxs {
			if v, ok := tx.PostCommitVolumes[account][p.Asset]; ok {
				a.txPreCommitVol[account][p.Asset] = v
			}
		}
	}

	if _, ok := a.txPreCommitVol[account][p.Asset]; !ok {
		var err error
		a.txPreCommitVol[account][p.Asset], err = l.store.GetVolumes(ctx, account, p.Asset)
		if err != nil {
			return err
		}
	}

	if _, ok := a.txPostCommitVol[account][p.Asset]; !ok {
		a.txPostCommitVol[account][p.Asset] = a.txPreCommitVol[account][p.Asset]
	}

	if _, ok := a.aggregatePreCommitVol[account][p.Asset]; !ok {
		a.aggregatePreCommitVol[account][p.Asset] = a.txPreCommitVol[account][p.Asset]
	}

	v := a.txPostCommitVol[account][p.Asset]
	v.Input += inputAmount
	v.Output += outputAmount
	a.txPostCommitVol[account][p.Asset] = v
	a.aggregatePostCommitVol[account][p.Asset] = a.txPostCommitVol[account][p.Asset]

	return nil
}

func (l *Ledger) checkPostCommitVolumes(ctx context.Context, txPostCommitVol core.AccountsAssetsVolumes,
	contracts []core.Contract, accMatchingContracts map[string]core.Account) error {
	for accountAddress, assetsVolumes := range txPostCommitVol {
		for asset, volumes := range assetsVolumes {
			if accountAddress == "world" {
				continue
			}

			for _, contract := range contracts {
				if contract.Match(accountAddress) {
					account, ok := accMatchingContracts[accountAddress]
					if !ok {
						var err error
						account, err = l.store.GetAccount(ctx, accountAddress)
						if err != nil {
							return err
						}
						accMatchingContracts[accountAddress] = account
					}

					if !contract.Expr.Eval(core.EvalContext{
						Variables: map[string]interface{}{
							"balance": float64(volumes.Balance()),
						},
						Metadata: account.Metadata,
						Asset:    asset,
					}) {
						return NewInsufficientFundError(asset)
					}
					break
				}
			}
		}
	}

	return nil
}

func initCommitVolumes(a volumeAggregator, p core.Posting) {
	if _, ok := a.aggregatePreCommitVol[p.Source]; !ok {
		a.aggregatePreCommitVol[p.Source] = core.AssetsVolumes{}
	}
	if _, ok := a.aggregatePreCommitVol[p.Destination]; !ok {
		a.aggregatePreCommitVol[p.Destination] = core.AssetsVolumes{}
	}
	if _, ok := a.aggregatePostCommitVol[p.Source]; !ok {
		a.aggregatePostCommitVol[p.Source] = core.AssetsVolumes{}
	}
	if _, ok := a.aggregatePostCommitVol[p.Destination]; !ok {
		a.aggregatePostCommitVol[p.Destination] = core.AssetsVolumes{}
	}

	if _, ok := a.txPreCommitVol[p.Source]; !ok {
		a.txPreCommitVol[p.Source] = core.AssetsVolumes{}
	}
	if _, ok := a.txPreCommitVol[p.Destination]; !ok {
		a.txPreCommitVol[p.Destination] = core.AssetsVolumes{}
	}

	if _, ok := a.txPostCommitVol[p.Source]; !ok {
		a.txPostCommitVol[p.Source] = core.AssetsVolumes{}
	}
	if _, ok := a.txPostCommitVol[p.Destination]; !ok {
		a.txPostCommitVol[p.Destination] = core.AssetsVolumes{}
	}
}
