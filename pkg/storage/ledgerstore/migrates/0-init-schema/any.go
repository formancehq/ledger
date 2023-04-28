package initschema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/aggregator"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/ledger/pkg/storage/migrations"
	"github.com/formancehq/ledger/pkg/storage/schema"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
)

func init() {
	migrations.RegisterGoMigration(UpgradeLogs)
}

const (
	LogTableName = "log"

	LogsMigrationBatchSizeFlag = "logs-migration-batch-size"
	OldSchemaRenameSuffixFlag  = "old-schema-rename-prefix"
)

var (
	batchSize             uint64 = 10000
	oldSchemaRenameSuffix        = "_save_v2_0_0"
)

func InitMigrationConfigCLIFlags(flags *flag.FlagSet) {
	flags.Uint64(LogsMigrationBatchSizeFlag, 10000, "Batch size for logs migration")
	flags.String(OldSchemaRenameSuffixFlag, "save_v2_0_0", "Name of the old schema (to be renamed)")
}

type Log struct {
	bun.BaseModel `bun:"log,alias:log"`

	ID   uint64          `bun:"id,unique,type:bigint"`
	Type string          `bun:"type,type:varchar"`
	Hash string          `bun:"hash,type:varchar"`
	Date core.Time       `bun:"date,type:timestamptz"`
	Data json.RawMessage `bun:"data,type:jsonb"`
}

func isLogTableExisting(
	ctx context.Context,
	schema schema.Schema,
	sqlTx *schema.Tx,
) (bool, error) {
	row := sqlTx.QueryRowContext(ctx, fmt.Sprintf(`
	SELECT EXISTS (
		SELECT FROM
			pg_tables
		WHERE
			schemaname = '%s' AND
			tablename  = 'log'
		)
	`, schema.Name()))
	if row.Err() != nil {
		return false, errors.Wrap(row.Err(), "checking if log table exists")
	}

	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, errors.Wrap(err, "scanning if log table exists")
	}

	return exists, nil
}

func readLogsRange(
	ctx context.Context,
	schema schema.Schema,
	sqlTx *schema.Tx,
	idMin, idMax uint64,
) ([]Log, error) {
	rawLogs := make([]Log, 0)
	sb := schema.
		NewSelect(LogTableName).
		Where("id >= ?", idMin).
		Where("id < ?", idMax).
		Model((*Log)(nil))

	rows, err := sqlTx.QueryContext(ctx, sb.String())
	if err != nil {
		if err == sql.ErrNoRows {
			return rawLogs, nil
		}

		return nil, errors.Wrap(err, "selecting logs")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			if err == sql.ErrNoRows {
				return
			}
			panic(err)
		}
	}()

	for rows.Next() {
		var log Log
		if err := rows.Scan(&log); err != nil {
			return nil, errors.Wrap(err, "scanning log")
		}

		rawLogs = append(rawLogs, log)
	}

	return rawLogs, nil
}

func (l *Log) ToLogsV2() (LogV2, error) {
	logType, err := core.LogTypeFromString(l.Type)
	if err != nil {
		return LogV2{}, errors.Wrap(err, "converting log type")
	}

	return LogV2{
		ID:   l.ID,
		Type: int16(logType),
		Hash: []byte(l.Hash),
		Date: l.Date,
		Data: l.Data,
	}, nil
}

type LogV2 struct {
	bun.BaseModel `bun:"logs_v2,alias:logs_v2"`

	ID   uint64    `bun:"id,unique,type:bigint"`
	Type int16     `bun:"type,type:smallint"`
	Hash []byte    `bun:"hash,type:varchar(256)"`
	Date core.Time `bun:"date,type:timestamptz"`
	Data []byte    `bun:"data,type:bytea"`
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func batchLogs(
	ctx context.Context,
	schema schema.Schema,
	sqlTx *schema.Tx,
	logs []LogV2,
) error {
	txn, err := sqlTx.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(pq.CopyInSchema(
		schema.Name(),
		"logs_v2",
		"id", "type", "hash", "date", "data",
	))
	if err != nil {
		return err
	}

	for _, l := range logs {
		_, err = stmt.Exec(l.ID, l.Type, l.Hash, l.Date, RawMessage(l.Data))
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return txn.Commit()
}

func cleanSchema(
	ctx context.Context,
	schemaV1 schema.Schema,
	schemaV2 schema.Schema,
	sqlTx *schema.Tx,
) error {
	_, err := sqlTx.ExecContext(ctx, fmt.Sprintf(`ALTER SCHEMA "%s" RENAME TO "%s"`,
		schemaV1.Name(), schemaV1.Name()+oldSchemaRenameSuffix))
	if err != nil {
		return err
	}

	_, err = sqlTx.ExecContext(ctx, fmt.Sprintf(`ALTER SCHEMA "%s" RENAME TO "%s"`,
		schemaV2.Name(), schemaV1.Name()))

	return err
}

func migrateLogs(
	ctx context.Context,
	schemaV1 schema.Schema,
	schemaV2 schema.Schema,
	sqlTx *schema.Tx,
) error {
	exists, err := isLogTableExisting(ctx, schemaV1, sqlTx)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	var idMin uint64
	var idMax uint64 = idMin + batchSize
	for {
		logs, err := readLogsRange(ctx, schemaV1, sqlTx, idMin, idMax)
		if err != nil {
			return err
		}

		if len(logs) == 0 {
			break
		}

		logsV2 := make([]LogV2, 0, len(logs))
		for _, l := range logs {
			logV2, err := l.ToLogsV2()
			if err != nil {
				return err
			}

			logsV2 = append(logsV2, logV2)
		}

		err = batchLogs(ctx, schemaV2, sqlTx, logsV2)
		if err != nil {
			return err
		}

		idMin = idMax
		idMax = idMin + batchSize
	}

	return nil
}

func processLogs(
	ctx context.Context,
	store *ledgerstore.Store,
	logs ...core.PersistedLog,
) error {
	logsData, err := buildData(ctx, store, logs...)
	if err != nil {
		return errors.Wrap(err, "building data")
	}

	if err := store.RunInTransaction(ctx, func(ctx context.Context, tx *ledgerstore.Store) error {
		if len(logsData.ensureAccountsExist) > 0 {
			if err := tx.EnsureAccountsExist(ctx, logsData.ensureAccountsExist); err != nil {
				return errors.Wrap(err, "ensuring accounts exist")
			}
		}
		if len(logsData.accountsToUpdate) > 0 {
			if err := tx.UpdateAccountsMetadata(ctx, logsData.accountsToUpdate); err != nil {
				return errors.Wrap(err, "updating accounts metadata")
			}
		}

		if len(logsData.transactionsToInsert) > 0 {
			if err := tx.InsertTransactions(ctx, logsData.transactionsToInsert...); err != nil {
				return errors.Wrap(err, "inserting transactions")
			}
		}

		if len(logsData.transactionsToUpdate) > 0 {
			if err := tx.UpdateTransactionsMetadata(ctx, logsData.transactionsToUpdate...); err != nil {
				return errors.Wrap(err, "updating transactions")
			}
		}

		if len(logsData.volumesToUpdate) > 0 {
			return tx.UpdateVolumes(ctx, logsData.volumesToUpdate...)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

type logsData struct {
	accountsToUpdate     []core.Account
	ensureAccountsExist  []string
	transactionsToInsert []core.ExpandedTransaction
	transactionsToUpdate []core.TransactionWithMetadata
	volumesToUpdate      []core.AccountsAssetsVolumes
}

func buildData(
	ctx context.Context,
	store *ledgerstore.Store,
	logs ...core.PersistedLog,
) (*logsData, error) {
	logsData := &logsData{}

	volumeAggregator := aggregator.Volumes(store)
	accountsToUpdate := make(map[string]metadata.Metadata)
	transactionsToUpdate := make(map[uint64]metadata.Metadata)

	for _, log := range logs {
		switch log.Type {
		case core.NewTransactionLogType:
			payload := log.Data.(core.NewTransactionLogPayload)
			txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.Transaction.Postings...)
			if err != nil {
				return nil, err
			}

			if payload.AccountMetadata != nil {
				for account, metadata := range payload.AccountMetadata {
					if m, ok := accountsToUpdate[account]; !ok {
						accountsToUpdate[account] = metadata
					} else {
						for k, v := range metadata {
							m[k] = v
						}
					}
				}
			}

			expandedTx := core.ExpandedTransaction{
				Transaction:       *payload.Transaction,
				PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
				PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
			}

			logsData.transactionsToInsert = append(logsData.transactionsToInsert, expandedTx)

			for account := range txVolumeAggregator.PostCommitVolumes {
				logsData.ensureAccountsExist = append(logsData.ensureAccountsExist, account)
			}

			logsData.volumesToUpdate = append(logsData.volumesToUpdate, txVolumeAggregator.PostCommitVolumes)

		case core.SetMetadataLogType:
			setMetadata := log.Data.(core.SetMetadataLogPayload)
			switch setMetadata.TargetType {
			case core.MetaTargetTypeAccount:
				addr := setMetadata.TargetID.(string)
				if m, ok := accountsToUpdate[addr]; !ok {
					accountsToUpdate[addr] = setMetadata.Metadata
				} else {
					for k, v := range setMetadata.Metadata {
						m[k] = v
					}
				}

			case core.MetaTargetTypeTransaction:
				id := setMetadata.TargetID.(uint64)
				if m, ok := transactionsToUpdate[id]; !ok {
					transactionsToUpdate[id] = setMetadata.Metadata
				} else {
					for k, v := range setMetadata.Metadata {
						m[k] = v
					}
				}
			}

		case core.RevertedTransactionLogType:
			payload := log.Data.(core.RevertedTransactionLogPayload)
			id := payload.RevertedTransactionID
			metadata := core.RevertedMetadata(payload.RevertTransaction.ID)
			if m, ok := transactionsToUpdate[id]; !ok {
				transactionsToUpdate[id] = metadata
			} else {
				for k, v := range metadata {
					m[k] = v
				}
			}

			txVolumeAggregator, err := volumeAggregator.NextTxWithPostings(ctx, payload.RevertTransaction.Postings...)
			if err != nil {
				return nil, errors.Wrap(err, "aggregating volumes")
			}

			expandedTx := core.ExpandedTransaction{
				Transaction:       *payload.RevertTransaction,
				PreCommitVolumes:  txVolumeAggregator.PreCommitVolumes,
				PostCommitVolumes: txVolumeAggregator.PostCommitVolumes,
			}
			logsData.transactionsToInsert = append(logsData.transactionsToInsert, expandedTx)
		}
	}

	for account, metadata := range accountsToUpdate {
		logsData.accountsToUpdate = append(logsData.accountsToUpdate, core.Account{
			Address:  account,
			Metadata: metadata,
		})
	}

	for transaction, metadata := range transactionsToUpdate {
		logsData.transactionsToUpdate = append(logsData.transactionsToUpdate, core.TransactionWithMetadata{
			ID:       transaction,
			Metadata: metadata,
		})
	}

	return logsData, nil
}

func initLedger(
	ctx context.Context,
	store *ledgerstore.Store,
) error {
	if !store.IsInitialized() {
		return nil
	}

	lastReadLogID, err := store.GetNextLogID(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return errors.Wrap(err, "reading last log")
	}

	for {
		logs, err := store.ReadLogsRange(ctx, lastReadLogID, lastReadLogID+uint64(batchSize))
		if err != nil {
			return errors.Wrap(err, "reading logs since last ID")
		}

		if len(logs) == 0 {
			// No logs, nothing to do
			return nil
		}

		if err := processLogs(ctx, store, logs...); err != nil {
			return errors.Wrap(err, "processing logs")
		}

		if err := store.UpdateNextLogID(ctx, logs[len(logs)-1].ID+1); err != nil {
			return errors.Wrap(err, "updating last read log")
		}
		lastReadLogID = logs[len(logs)-1].ID + 1

		if uint64(len(logs)) < batchSize {
			// Nothing to do anymore, no need to read more logs
			return nil
		}
	}
}

func UpgradeLogs(
	ctx context.Context,
	schemaV1 schema.Schema,
	sqlTx *schema.Tx,
) error {
	b := viper.GetUint64(LogsMigrationBatchSizeFlag)
	if b != 0 {
		batchSize = b
	}

	suffix := viper.GetString(OldSchemaRenameSuffixFlag)
	if suffix != "" {
		oldSchemaRenameSuffix = suffix
	}

	// Create schema v2
	schemaV2 := schema.NewSchema(sqlTx.Tx, schemaV1.Name()+"_v2_0_0")
	store, err := ledgerstore.NewStore(
		schemaV2,
		func(ctx context.Context) error {
			return nil
		},
		ledgerstore.DefaultStoreConfig,
	)
	if err != nil {
		return errors.Wrap(err, "creating store")
	}

	if err := migrateLogs(ctx, schemaV1, schemaV2, sqlTx); err != nil {
		return errors.Wrap(err, "migrating logs")
	}

	if err := initLedger(ctx, store); err != nil {
		return errors.Wrap(err, "initializing ledger")
	}

	return cleanSchema(ctx, schemaV1, schemaV2, sqlTx)
}
