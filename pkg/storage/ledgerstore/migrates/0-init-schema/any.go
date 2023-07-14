package initschema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/ledger/pkg/storage/migrations"
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
	schema storage.Schema,
	sqlTx *storage.Tx,
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
	sqlTx *storage.Tx,
	idMin, idMax uint64,
) ([]Log, error) {
	rawLogs := make([]Log, 0)
	if err := sqlTx.
		NewSelect(LogTableName).
		Where("id >= ?", idMin).
		Where("id < ?", idMax).
		Model(&rawLogs).
		Scan(ctx); err != nil {
		return nil, err
	}

	return rawLogs, nil
}

func convertMetadata(data []byte) any {
	ret := make(map[string]any)
	if err := json.Unmarshal(data, &ret); err != nil {
		panic(err)
	}
	oldMetadata := ret["metadata"].(map[string]any)
	newMetadata := make(map[string]string)
	for k, v := range oldMetadata {
		newMetadata[k] = fmt.Sprint(v)
	}
	ret["metadata"] = newMetadata

	return ret
}

func (l *Log) ToLogsV2() (ledgerstore.LogsV2, error) {
	logType, err := core.LogTypeFromString(l.Type)
	if err != nil {
		return ledgerstore.LogsV2{}, errors.Wrap(err, "converting log type")
	}

	var data any
	switch logType {
	case core.NewTransactionLogType:
		data = map[string]any{
			"transaction":     convertMetadata(l.Data),
			"accountMetadata": map[string]any{},
		}
	case core.SetMetadataLogType:
		data = convertMetadata(l.Data)
	case core.RevertedTransactionLogType:
		data = l.Data
	default:
		panic("unknown type " + logType.String())
	}

	asJson, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return ledgerstore.LogsV2{
		ID:   l.ID,
		Type: int16(logType),
		Hash: []byte(l.Hash),
		Date: l.Date,
		Data: asJson,
	}, nil
}

func batchLogs(
	ctx context.Context,
	schema storage.Schema,
	sqlTx *storage.Tx,
	logs []ledgerstore.LogsV2,
) error {
	txn, err := sqlTx.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := txn.Prepare(pq.CopyInSchema(
		schema.Name(),
		ledgerstore.LogTableName,
		"id", "type", "hash", "date", "data",
	))
	if err != nil {
		return err
	}

	for _, l := range logs {
		_, err = stmt.Exec(l.ID, l.Type, l.Hash, l.Date, ledgerstore.RawMessage(l.Data))
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
	schemaV1 storage.Schema,
	schemaV2 storage.Schema,
	sqlTx *storage.Tx,
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
	schemaV1 storage.Schema,
	schemaV2 storage.Schema,
	store *ledgerstore.Store,
	sqlTx *storage.Tx,
) error {
	exists, err := isLogTableExisting(ctx, schemaV1, sqlTx)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	var idMin uint64
	var idMax = idMin + batchSize
	for {
		logs, err := readLogsRange(ctx, sqlTx, idMin, idMax)
		if err != nil {
			return err
		}

		if len(logs) == 0 {
			break
		}

		logsV2 := make([]ledgerstore.LogsV2, 0, len(logs))
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

		for _, log := range logsV2 {
			coreLog := log.ToCore()
			switch payload := coreLog.Data.(type) {
			case core.NewTransactionLogPayload:
				if err := store.InsertTransactions(ctx, *payload.Transaction); err != nil {
					return err
				}
			case core.SetMetadataLogPayload:
				switch payload.TargetType {
				case core.MetaTargetTypeTransaction:
					if err := store.UpdateTransactionsMetadata(ctx, core.TransactionWithMetadata{
						ID:       payload.TargetID.(uint64),
						Metadata: payload.Metadata,
					}); err != nil {
						return err
					}
				case core.MetaTargetTypeAccount:
					if err := store.UpdateAccountsMetadata(ctx, core.Account{
						Address:  payload.TargetID.(string),
						Metadata: payload.Metadata,
					}); err != nil {
						return err
					}
				}
			case core.RevertedTransactionLogPayload:
				if err := store.InsertTransactions(ctx, *payload.RevertTransaction); err != nil {
					return err
				}
				if err := store.UpdateTransactionsMetadata(ctx, core.TransactionWithMetadata{
					ID:       payload.RevertedTransactionID,
					Metadata: core.RevertedMetadata(payload.RevertTransaction.ID),
				}); err != nil {
					return err
				}
			}
			if err := store.MarkedLogsAsProjected(ctx, log.ID); err != nil {
				return err
			}
		}

		idMin = idMax
		idMax = idMin + batchSize
	}

	return nil
}

func UpgradeLogs(
	ctx context.Context,
	schemaV1 storage.Schema,
	sqlTx *storage.Tx,
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
	schemaV2 := storage.NewSchema(sqlTx.Tx, schemaV1.Name()+"_v2_0_0")
	store, err := ledgerstore.New(
		schemaV2,
		func(ctx context.Context) error {
			return nil
		},
	)
	if err != nil {
		return errors.Wrap(err, "creating store")
	}

	if err := migrateLogs(ctx, schemaV1, schemaV2, store, sqlTx); err != nil {
		return errors.Wrap(err, "migrating logs")
	}

	return cleanSchema(ctx, schemaV1, schemaV2, sqlTx)
}
