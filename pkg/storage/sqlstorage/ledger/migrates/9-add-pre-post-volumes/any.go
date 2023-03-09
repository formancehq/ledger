package add_pre_post_volumes

import (
	"context"
	"encoding/json"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

func init() {
	migrations.RegisterGoMigration(Upgrade)
}

type Transaction struct {
	ID       uint64        `json:"txid"`
	Postings core.Postings `json:"postings"`
}

func Upgrade(ctx context.Context, schema schema.Schema, sqlTx *bun.Tx) error {
	sb := schema.NewSelect(ledger.LogTableName).
		Model((*ledger.Log)(nil)).
		Column("data").
		Where("type = ?", core.NewTransactionType).
		Order("id ASC")

	rows, err := sqlTx.QueryContext(ctx, sb.String())
	if err != nil {
		return errors.Wrap(err, "querying rows")
	}
	defer rows.Close()

	updates := make([]*bun.UpdateQuery, 0)

	aggregatedVolumes := core.AccountsAssetsVolumes{}
	for rows.Next() {
		var data string
		err := rows.Scan(&data)
		if err != nil {
			return errors.Wrap(err, "scanning row")
		}

		var tx Transaction
		err = json.Unmarshal([]byte(data), &tx)
		if err != nil {
			return errors.Wrap(err, "decoding transaction")
		}

		preCommitVolumes := core.AccountsAssetsVolumes{}
		postCommitVolumes := core.AccountsAssetsVolumes{}
		for _, posting := range tx.Postings {

			preCommitVolumes.SetVolumes(
				posting.Source,
				posting.Asset,
				aggregatedVolumes.GetVolumes(posting.Source, posting.Asset),
			)

			preCommitVolumes.SetVolumes(
				posting.Destination,
				posting.Asset,
				aggregatedVolumes.GetVolumes(posting.Destination, posting.Asset),
			)

			if !postCommitVolumes.HasAccount(posting.Source) {
				postCommitVolumes.SetVolumes(
					posting.Source,
					posting.Asset,
					preCommitVolumes.GetVolumes(posting.Source, posting.Asset),
				)
			}

			if !postCommitVolumes.HasAccount(posting.Destination) {
				postCommitVolumes.SetVolumes(
					posting.Destination,
					posting.Asset,
					preCommitVolumes.GetVolumes(posting.Destination, posting.Asset),
				)
			}

			postCommitVolumes.AddOutput(
				posting.Source,
				posting.Asset,
				posting.Amount,
			)

			postCommitVolumes.AddInput(
				posting.Destination,
				posting.Asset,
				posting.Amount,
			)
		}

		for account, accountVolumes := range postCommitVolumes {
			for asset, volumes := range accountVolumes {
				aggregatedVolumes.SetVolumes(account, asset, core.Volumes{
					Input:  volumes.Input.OrZero(),
					Output: volumes.Output.OrZero(),
				})
			}
		}

		ub := schema.NewUpdate(ledger.TransactionsTableName).
			Model((*ledger.Transactions)(nil)).
			Set("pre_commit_volumes = ?", preCommitVolumes).
			Set("post_commit_volumes = ?", postCommitVolumes).
			Where("id = ?", tx.ID)

		updates = append(updates, ub)
	}
	err = rows.Close()
	if err != nil {
		return err
	}

	for _, update := range updates {
		_, err = sqlTx.ExecContext(ctx, update.String())
		if err != nil {
			return errors.Wrap(err, "executing update")
		}
	}

	return nil
}
