package add_pre_post_volumes

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pkg/errors"
)

func init() {
	sqlstorage.RegisterGoMigration(Upgrade)
}

type Transaction struct {
	ID       uint64        `json:"txid"`
	Postings core.Postings `json:"postings"`
}

func Upgrade(ctx context.Context, schema sqlstorage.Schema, sqlTx *sql.Tx) error {
	sb := sqlbuilder.NewSelectBuilder()
	sb.From(schema.Table("log"))
	sb.Select("data")
	sb.Where(sb.E("type", core.NewTransactionType))
	sb.OrderBy("id")
	sb.Asc()

	sqlq, args := sb.BuildWithFlavor(schema.Flavor())
	rows, err := sqlTx.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return errors.Wrap(err, "querying rows")
	}
	defer rows.Close()

	updates := make([]*sqlbuilder.UpdateBuilder, 0)

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
			if _, ok := preCommitVolumes[posting.Source]; !ok {
				preCommitVolumes[posting.Source] = core.AssetsVolumes{}
			}
			if _, ok := preCommitVolumes[posting.Destination]; !ok {
				preCommitVolumes[posting.Destination] = core.AssetsVolumes{}
			}
			preCommitVolumes[posting.Source][posting.Asset] = aggregatedVolumes.GetVolumes(posting.Source, posting.Asset)
			preCommitVolumes[posting.Destination][posting.Asset] = aggregatedVolumes.GetVolumes(posting.Destination, posting.Asset)

			if _, ok := postCommitVolumes[posting.Source]; !ok {
				postCommitVolumes.SetVolumes(posting.Source, posting.Asset,
					preCommitVolumes.GetVolumes(posting.Source, posting.Asset))

			}
			if _, ok := postCommitVolumes[posting.Destination]; !ok {
				postCommitVolumes.SetVolumes(posting.Destination, posting.Asset,
					preCommitVolumes.GetVolumes(posting.Destination, posting.Asset))
			}

			postCommitVolumes.AddOutput(posting.Source, posting.Asset, posting.Amount)
			postCommitVolumes.AddInput(posting.Destination, posting.Asset, posting.Amount)
		}

		for account, accountVolumes := range postCommitVolumes {
			for asset, volumes := range accountVolumes {
				aggregatedVolumes.SetVolumes(account, asset, volumes)
			}
		}

		preCommitVolumesData, err := json.Marshal(preCommitVolumes)
		if err != nil {
			return err
		}

		postCommitVolumesData, err := json.Marshal(postCommitVolumes)
		if err != nil {
			return err
		}

		ub := sqlbuilder.NewUpdateBuilder()
		ub.Update(schema.Table("transactions"))
		ub.Set(
			ub.Assign("pre_commit_volumes", preCommitVolumesData),
			ub.Assign("post_commit_volumes", postCommitVolumesData),
		)
		ub.Where(ub.E("id", tx.ID))

		updates = append(updates, ub)
	}
	err = rows.Close()
	if err != nil {
		return err
	}

	for _, update := range updates {
		sqlq, args := update.BuildWithFlavor(schema.Flavor())

		_, err = sqlTx.ExecContext(ctx, sqlq, args...)
		if err != nil {
			return errors.Wrap(err, "executing update")
		}
	}

	return nil
}
