package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (l *Ledger) ExecuteTxsData(ctx context.Context, preview bool, txsData ...core.TransactionData) ([]core.ExpandedTransaction, error) {
	ctx, span := opentelemetry.Start(ctx, "ExecuteTxsData")
	defer span.End()

	if len(txsData) == 0 {
		return []core.ExpandedTransaction{}, errors.New("no transaction data to execute")
	}

	lastTx, err := l.store.GetLastTransaction(ctx)
	if err != nil {
		return []core.ExpandedTransaction{}, errors.Wrap(err,
			"could not get last transaction")
	}

	vAggr := NewVolumeAggregator(l)
	txs := make([]core.ExpandedTransaction, 0)
	var nextTxId uint64
	var lastTxTimestamp time.Time
	if lastTx != nil {
		nextTxId = lastTx.ID + 1
		lastTxTimestamp = lastTx.Timestamp
	}

	contracts := make([]core.Contract, 0)
	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return []core.ExpandedTransaction{}, errors.Wrap(err,
			"loading mapping")
	}
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	usedReferences := make(map[string]struct{})
	accs := map[string]*core.AccountWithVolumes{}
	for i, txData := range txsData {
		if len(txData.Postings) == 0 {
			return []core.ExpandedTransaction{}, NewValidationError(
				fmt.Sprintf("executing transaction data %d: no postings", i))
		}
		// Until v1.5.0, dates was stored as string using rfc3339 format
		// So round the date to the second to keep the same behaviour
		if txData.Timestamp.IsZero() {
			txData.Timestamp = time.Now().UTC().Truncate(time.Second)
		} else {
			txData.Timestamp = txData.Timestamp.UTC()
		}

		past := false
		if lastTx != nil && txData.Timestamp.Before(lastTxTimestamp) {
			past = true
		}
		if past && !l.allowPastTimestamps {
			return []core.ExpandedTransaction{}, NewValidationError(fmt.Sprintf(
				"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
				txData.Timestamp.Format(time.RFC3339Nano),
				lastTxTimestamp.Sub(txData.Timestamp),
				lastTxTimestamp.Format(time.RFC3339Nano)))
		}
		lastTxTimestamp = txData.Timestamp

		if txData.Reference != "" {
			if _, ok := usedReferences[txData.Reference]; ok {
				return []core.ExpandedTransaction{}, NewConflictError()
			}
			usedReferences[txData.Reference] = struct{}{}

			txs, err := l.GetTransactions(ctx, *NewTransactionsQuery().
				WithReferenceFilter(txData.Reference))
			if err != nil {
				return []core.ExpandedTransaction{}, errors.Wrap(err, "GetTransactions")
			}
			if len(txs.Data) > 0 {
				return []core.ExpandedTransaction{}, NewConflictError()
			}
		}

		txVolumeAggr := vAggr.NextTx()
		for _, posting := range txData.Postings {
			if err := txVolumeAggr.Transfer(ctx,
				posting.Source, posting.Destination, posting.Asset, posting.Amount, accs); err != nil {
				return []core.ExpandedTransaction{}, NewTransactionCommitError(i, err)
			}
		}

		for account, volumes := range txVolumeAggr.PostCommitVolumes {
			if _, ok := accs[account]; !ok {
				accs[account], err = l.GetAccount(ctx, account)
				if err != nil {
					return []core.ExpandedTransaction{}, NewTransactionCommitError(i,
						errors.Wrap(err, fmt.Sprintf("GetAccount '%s'", account)))
				}
			}
			for asset, vol := range volumes {
				accs[account].Volumes[asset] = vol
			}
			accs[account].Balances = accs[account].Volumes.Balances()
			for asset, volume := range volumes {
				if account == core.WORLD {
					continue
				}

				for _, contract := range contracts {
					if contract.Match(account) {
						if ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": volume.Balance(),
							},
							Metadata: accs[account].Metadata,
							Asset:    asset,
						}); !ok {
							return []core.ExpandedTransaction{}, NewInsufficientFundError(asset)
						}
						break
					}
				}
			}
		}

		if txData.Metadata == nil {
			txData.Metadata = core.Metadata{}
		}

		tx := core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: txData,
				ID:              nextTxId,
			},
			PreCommitVolumes:  txVolumeAggr.PreCommitVolumes,
			PostCommitVolumes: txVolumeAggr.PostCommitVolumes,
		}
		lastTx = &tx
		txs = append(txs, tx)
		nextTxId++
	}

	if preview {
		return txs, nil
	}

	if err := l.store.Commit(ctx, txs...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return []core.ExpandedTransaction{}, NewConflictError()
		default:
			return []core.ExpandedTransaction{}, errors.Wrap(err,
				"committing transactions")
		}
	}

	l.monitor.CommittedTransactions(ctx, l.store.Name(), txs...)
	return txs, nil
}
