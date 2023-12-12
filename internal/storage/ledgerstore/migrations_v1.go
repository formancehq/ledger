package ledgerstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

var (
	batchSize             uint64 = 10000
	oldSchemaRenameSuffix        = "_save_v2_0_0"
)

type LogV1 struct {
	ID   uint64          `bun:"id,unique,type:bigint"`
	Type string          `bun:"type,type:varchar"`
	Hash string          `bun:"hash,type:varchar"`
	Date ledger.Time     `bun:"date,type:timestamptz"`
	Data json.RawMessage `bun:"data,type:jsonb"`
}

func readLogsRange(
	ctx context.Context,
	schema string,
	sqlTx bun.Tx,
	idMin, idMax uint64,
) ([]LogV1, error) {
	rawLogs := make([]LogV1, 0)
	if err := sqlTx.
		NewSelect().
		Table(fmt.Sprintf(`%s.log`, schema)).
		Where("id >= ?", idMin).
		Where("id < ?", idMax).
		Scan(ctx, &rawLogs); err != nil {
		return nil, err
	}

	return rawLogs, nil
}

func convertMetadata(ret map[string]any) map[string]any {
	oldMetadata := ret["metadata"].(map[string]any)
	newMetadata := make(map[string]string)
	for k, v := range oldMetadata {
		switch v := v.(type) {
		case map[string]any:
			if len(v) == 2 && v["type"] != nil && v["value"] != nil {
				switch v["type"] {
				case "asset", "string", "account":
					newMetadata[k] = v["value"].(string)
				case "monetary":
					newMetadata[k] = fmt.Sprintf("%s %d",
						v["value"].(map[string]any)["asset"].(string),
						int(v["value"].(map[string]any)["amount"].(float64)),
					)
				case "portion":
					newMetadata[k] = v["value"].(map[string]any)["specific"].(string)
				case "number":
					newMetadata[k] = fmt.Sprint(v["value"])
				}
			} else {
				newMetadata[k] = fmt.Sprint(v)
			}
		default:
			newMetadata[k] = fmt.Sprint(v)
		}
	}
	ret["metadata"] = newMetadata

	return ret
}

func convertTransaction(ret map[string]any) map[string]any {
	ret = convertMetadata(ret)
	ret["id"] = ret["txid"]
	delete(ret, "txid")

	return ret
}

func (l *LogV1) ToLogsV2() (Logs, error) {
	logType := ledger.LogTypeFromString(l.Type)

	ret := make(map[string]any)
	if err := json.Unmarshal(l.Data, &ret); err != nil {
		panic(err)
	}

	var data any
	switch logType {
	case ledger.NewTransactionLogType:
		data = map[string]any{
			"transaction":     convertTransaction(ret),
			"accountMetadata": map[string]any{},
		}
	case ledger.SetMetadataLogType:
		data = convertMetadata(ret)
	case ledger.RevertedTransactionLogType:
		data = l.Data
	default:
		panic("unknown type " + logType.String())
	}

	asJson, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return Logs{
		ID:   (*bunpaginate.BigInt)(big.NewInt(int64(l.ID))),
		Type: logType.String(),
		Hash: []byte(l.Hash),
		Date: l.Date,
		Data: asJson,
	}, nil
}

func batchLogs(
	ctx context.Context,
	schema string,
	sqlTx bun.Tx,
	logs []Logs,
) error {
	// Beware: COPY query is not supported by bun if the pgx driver is used.
	stmt, err := sqlTx.PrepareContext(ctx, pq.CopyInSchema(
		schema,
		"logs",
		"id", "type", "hash", "date", "data",
	))
	if err != nil {
		return err
	}

	for _, l := range logs {
		_, err = stmt.ExecContext(ctx, l.ID, l.Type, l.Hash, l.Date, RawMessage(l.Data))
		if err != nil {
			return err
		}
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return nil
}

func migrateLogs(
	ctx context.Context,
	schemaV1Name string,
	schemaV2Name string,
	sqlTx bun.Tx,
) error {

	var idMin uint64
	var idMax = idMin + batchSize
	for {
		logs, err := readLogsRange(ctx, schemaV1Name, sqlTx, idMin, idMax)
		if err != nil {
			return errors.Wrap(err, "reading logs from old table")
		}

		if len(logs) == 0 {
			break
		}

		logsV2 := make([]Logs, 0, len(logs))
		for _, l := range logs {
			logV2, err := l.ToLogsV2()
			if err != nil {
				return err
			}

			logsV2 = append(logsV2, logV2)
		}

		err = batchLogs(ctx, schemaV2Name, sqlTx, logsV2)
		if err != nil {
			return err
		}

		idMin = idMax
		idMax = idMin + batchSize
	}

	return nil
}
