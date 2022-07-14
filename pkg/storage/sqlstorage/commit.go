package sqlstorage

import (
	"context"
	"database/sql"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (s *Store) commit(ctx context.Context, exec executor, txs ...core.Transaction) ([]core.Log, error) {
	if err := s.insertTransactions(ctx, exec, txs...); err != nil {
		return nil, errors.Wrap(err, "inserting transactions")
	}

	postCommitVolumes := core.AggregatePostCommitVolumes(txs...)

	for account := range postCommitVolumes {
		err := s.ensureAccountExists(ctx, exec, account)
		if err != nil {
			return nil, errors.Wrap(err, "creating account entries")
		}
	}

	if err := s.updateVolumes(ctx, exec, postCommitVolumes); err != nil {
		return nil, errors.Wrap(err, "updating volumes")
	}

	logs := make([]core.Log, 0)
	lastLog, err := s.lastLog(ctx, exec)
	if err != nil {
		return nil, err
	}
	for _, tx := range txs {
		newLog := core.NewTransactionLog(lastLog, tx)
		lastLog = &newLog
		logs = append(logs, newLog)
	}

	if err := s.appendLog(ctx, exec, logs...); err != nil {
		return nil, errors.Wrap(err, "inserting logs")
	}

	return logs, nil
}

func (s *Store) Commit(ctx context.Context, txs ...core.Transaction) error {
	sqlTx, err := s.schema.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(sqlTx)

	_, err = s.commit(ctx, sqlTx, txs...)
	if err != nil {
		return err
	}

	if err = sqlTx.Commit(); err != nil {
		return s.error(err)
	}

	return nil
}

func (s *Store) CommitRevert(ctx context.Context, reverted, revert core.Transaction) error {
	sqlTx, err := s.schema.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(sqlTx)

	logs, err := s.commit(ctx, sqlTx, revert)
	if err != nil {
		return err
	}

	at, err := time.Parse(time.RFC3339, revert.Timestamp)
	if err != nil {
		return err
	}

	metadata := core.RevertedMetadata(revert.ID)
	if err := s.updateTransactionMetadata(ctx, sqlTx, reverted.ID, metadata); err != nil {
		return err
	}

	if err := s.appendLog(ctx, sqlTx, core.NewSetMetadataLog(&logs[len(logs)-1], at, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   reverted.ID,
		Metadata:   metadata,
	})); err != nil {
		return err
	}

	if err = sqlTx.Commit(); err != nil {
		return s.error(err)
	}

	return nil
}
