package sqlstorage

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func (s *Store) commit(ctx context.Context, txs ...core.ExpandedTransaction) ([]core.Log, error) {
	if err := s.insertTransactions(ctx, txs...); err != nil {
		return nil, errors.Wrap(err, "inserting transactions")
	}

	postCommitVolumes := core.AggregatePostCommitVolumes(txs...)

	for account := range postCommitVolumes {
		err := s.ensureAccountExists(ctx, account)
		if err != nil {
			return nil, errors.Wrap(err, "ensuring account exists")
		}
	}

	if err := s.updateVolumes(ctx, postCommitVolumes); err != nil {
		return nil, errors.Wrap(err, "updating volumes")
	}

	logs := make([]core.Log, 0)
	lastLog, err := s.GetLastLog(ctx)
	if err != nil {
		return nil, err
	}

	for _, tx := range txs {
		newLog := core.NewTransactionLog(lastLog, tx.Transaction)
		lastLog = &newLog
		logs = append(logs, newLog)
	}

	if err := s.appendLog(ctx, logs...); err != nil {
		return nil, errors.Wrap(err, "inserting logs")
	}

	return logs, nil
}

func (s *Store) Commit(ctx context.Context, txs ...core.ExpandedTransaction) (err error) {
	if !storage.IsTransactional(ctx) {
		ctx = storage.TransactionalContext(ctx)
		defer func() {
			if err == nil {
				if commitErr := storage.CommitTransaction(ctx); commitErr != nil {
					panic(commitErr)
				}
			} else {
				if rollbackErr := storage.RollbackTransaction(ctx); rollbackErr != nil {
					panic(rollbackErr)
				}
			}
		}()
	}
	_, err = s.commit(ctx, txs...)
	return err
}
