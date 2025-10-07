package ledger

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/time"
)

type TransactionsSum struct {
	Asset string `json:"asset"`
	Sum   string `json:"sum"`
}

func (s *Store) TransactionsSum(ctx context.Context, ledger string, account string) ([]TransactionsSum, error) {
	return s.TransactionsSumWithTimeRange(ctx, ledger, account, nil, nil)
}

func (s *Store) TransactionsSumWithTimeRange(ctx context.Context, ledger string, account string, startTime, endTime *time.Time) ([]TransactionsSum, error) {
	whereClause := "ledger = ? AND accounts_address = ?"
	args := []any{ledger, account}

	if startTime != nil {
		whereClause += " AND effective_date >= ?"
		args = append(args, startTime)
	}

	if endTime != nil {
		whereClause += " AND effective_date <= ?"
		args = append(args, endTime)
	}

	query := fmt.Sprintf("SELECT asset, SUM(CASE WHEN is_source THEN -amount::numeric ELSE amount::numeric END)::text as sum FROM %s WHERE %s GROUP BY asset", s.GetPrefixedRelationName("moves"), whereClause)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TransactionsSum
	for rows.Next() {
		var asset string
		var sum string
		if err := rows.Scan(&asset, &sum); err != nil {
			return nil, err
		}
		results = append(results, TransactionsSum{
			Asset: asset,
			Sum:   sum,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
