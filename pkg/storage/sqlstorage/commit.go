package sqlstorage

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (s *Store) Commit(ctx context.Context, txs ...core.ExpandedTransaction) error {
	if err := s.insertTransactions(ctx, txs...); err != nil {
		return errors.Wrap(err, "inserting transactions")
	}

	postCommitVolumes := core.AggregatePostCommitVolumes(txs...)

	for account := range postCommitVolumes {
		err := s.ensureAccountExists(ctx, account)
		if err != nil {
			return errors.Wrap(err, "ensuring account exists")
		}
	}

	if err := s.updateVolumes(ctx, postCommitVolumes); err != nil {
		return errors.Wrap(err, "updating volumes")
	}

	logs := make([]core.Log, 0)
	lastLog, err := s.GetLastLog(ctx)
	if err != nil {
		return err
	}

	for _, tx := range txs {
		newLog := core.NewTransactionLog(lastLog, tx.Transaction)
		lastLog = &newLog
		logs = append(logs, newLog)
	}

	if err := s.appendLog(ctx, logs...); err != nil {
		return errors.Wrap(err, "inserting logs")
	}

	return nil
}
