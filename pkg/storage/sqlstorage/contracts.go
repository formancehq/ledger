package sqlstorage

import (
	"context"
	"encoding/json"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/sirupsen/logrus"
)

func (s *Store) FindContracts(ctx context.Context) ([]core.Contract, error) {
	results := make([]core.Contract, 0)
	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("contract_id", "contract_expr", "contract_account").
		From(s.table("contract"))

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	logrus.Debugln(sqlq, args)

	rows, err := s.db.QueryContext(
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		return nil, s.error(err)
	}

	for rows.Next() {
		var (
			id         string
			exprString string
			account    string
		)

		err := rows.Scan(&id, &exprString, &account)
		if err != nil {
			return nil, err
		}

		expr, err := core.ParseRule(exprString)
		if err != nil {
			return nil, err
		}

		contract := core.Contract{
			ID:      id,
			Expr:    expr,
			Account: account,
		}
		results = append(results, contract)
	}

	return results, nil
}

func (s *Store) SaveContract(ctx context.Context, contract core.Contract) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return s.error(err)
	}

	data, err := json.Marshal(contract.Expr)
	if err != nil {
		return err
	}

	ib := sqlbuilder.NewInsertBuilder()
	ib.InsertInto(s.table("contract"))
	ib.Cols("contract_id", "contract_account", "contract_expr")
	ib.Values(contract.ID, contract.Account, string(data))

	sqlq, args := ib.BuildWithFlavor(s.flavor)
	_, err = tx.ExecContext(ctx, sqlq, args...)
	if err != nil {
		tx.Rollback()

		return s.error(err)
	}
	return tx.Commit()
}
