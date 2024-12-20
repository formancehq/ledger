package legacy

import (
	"context"
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"regexp"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

var (
	metadataRegex = regexp.MustCompile(`metadata\[(.+)]`)
)

func (store *Store) buildTransactionQuery(p PITFilterWithVolumes, query *bun.SelectQuery) *bun.SelectQuery {

	selectMetadata := query.NewSelect().
		ModelTableExpr(store.GetPrefixedRelationName("transactions_metadata")).
		Where("transactions.seq = transactions_metadata.transactions_seq").
		Order("revision desc").
		Limit(1)

	if p.PIT != nil && !p.PIT.IsZero() {
		selectMetadata = selectMetadata.Where("date <= ?", p.PIT)
	}

	query = query.
		ModelTableExpr(store.GetPrefixedRelationName("transactions")).
		Where("transactions.ledger = ?", store.name)

	if p.PIT != nil && !p.PIT.IsZero() {
		query = query.
			Where("timestamp <= ?", p.PIT).
			Column("id", "inserted_at", "timestamp", "postings").
			Column("transactions_metadata.metadata").
			Join(fmt.Sprintf(`left join lateral (%s) as transactions_metadata on true`, selectMetadata.String())).
			ColumnExpr(fmt.Sprintf("case when reverted_at is not null and reverted_at > '%s' then null else reverted_at end", p.PIT.Format(time.DateFormat)))
	} else {
		query = query.Column(
			"transactions.metadata",
			"transactions.id",
			"transactions.inserted_at",
			"transactions.timestamp",
			"transactions.postings",
			"transactions.reverted_at",
			"transactions.reference",
		)
	}

	if p.ExpandEffectiveVolumes {
		query = query.ColumnExpr(store.GetPrefixedRelationName("get_aggregated_effective_volumes_for_transaction")+"(?, transactions.seq) as post_commit_effective_volumes", store.name)
	}
	if p.ExpandVolumes {
		query = query.ColumnExpr(store.GetPrefixedRelationName("get_aggregated_volumes_for_transaction")+"(?, transactions.seq) as post_commit_volumes", store.name)
	}
	return query
}

func (store *Store) transactionQueryContext(qb query.Builder, q ListTransactionsQuery) (string, []any, error) {

	return qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
		switch {
		case key == "reference" || key == "timestamp":
			return fmt.Sprintf("%s %s ?", key, query.DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
		case key == "reverted":
			if operator != "$match" {
				return "", nil, newErrInvalidQuery("'reverted' column can only be used with $match")
			}
			switch value := value.(type) {
			case bool:
				ret := "reverted_at is"
				if value {
					ret += " not"
				}
				return ret + " null", nil, nil
			default:
				return "", nil, newErrInvalidQuery("'reverted' can only be used with bool value")
			}
		case key == "account":
			if operator != "$match" {
				return "", nil, newErrInvalidQuery("'account' column can only be used with $match")
			}
			switch address := value.(type) {
			case string:
				return filterAccountAddressOnTransactions(address, true, true), nil, nil
			default:
				return "", nil, newErrInvalidQuery("unexpected type %T for column 'account'", address)
			}
		case key == "source":
			if operator != "$match" {
				return "", nil, errors.New("'source' column can only be used with $match")
			}
			switch address := value.(type) {
			case string:
				return filterAccountAddressOnTransactions(address, true, false), nil, nil
			default:
				return "", nil, newErrInvalidQuery("unexpected type %T for column 'source'", address)
			}
		case key == "destination":
			if operator != "$match" {
				return "", nil, errors.New("'destination' column can only be used with $match")
			}
			switch address := value.(type) {
			case string:
				return filterAccountAddressOnTransactions(address, false, true), nil, nil
			default:
				return "", nil, newErrInvalidQuery("unexpected type %T for column 'destination'", address)
			}
		case metadataRegex.Match([]byte(key)):
			if operator != "$match" {
				return "", nil, newErrInvalidQuery("'account' column can only be used with $match")
			}
			match := metadataRegex.FindAllStringSubmatch(key, 3)

			key := "metadata"
			if q.Options.Options.PIT != nil && !q.Options.Options.PIT.IsZero() {
				key = "transactions_metadata.metadata"
			}

			return key + " @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil

		case key == "metadata":
			if operator != "$exists" {
				return "", nil, newErrInvalidQuery("'metadata' key filter can only be used with $exists")
			}
			if q.Options.Options.PIT != nil && !q.Options.Options.PIT.IsZero() {
				key = "transactions_metadata.metadata"
			}

			return fmt.Sprintf("%s -> ? IS NOT NULL", key), []any{value}, nil
		default:
			return "", nil, newErrInvalidQuery("unknown key '%s' when building query", key)
		}
	}))
}

func (store *Store) buildTransactionListQuery(selectQuery *bun.SelectQuery, q ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes], where string, args []any) *bun.SelectQuery {

	selectQuery = store.buildTransactionQuery(q.Options, selectQuery)
	if where != "" {
		return selectQuery.Where(where, args...)
	}

	return selectQuery
}

func (store *Store) GetTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {

	var (
		where string
		args  []any
		err   error
	)
	if q.Options.QueryBuilder != nil {
		where, args, err = store.transactionQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return nil, err
		}
	}

	return paginateWithColumn[ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes], ledger.Transaction](store, ctx,
		(*bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return store.buildTransactionListQuery(query, q.Options, where, args)
		},
	)
}

func (store *Store) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {

	var (
		where string
		args  []any
		err   error
	)

	if q.Options.QueryBuilder != nil {
		where, args, err = store.transactionQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return 0, err
		}
	}

	return count[ledger.Transaction](store, true, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return store.buildTransactionListQuery(query, q.Options, where, args)
	})
}

func (store *Store) GetTransactionWithVolumes(ctx context.Context, filter GetTransactionQuery) (*ledger.Transaction, error) {
	return fetch[*ledger.Transaction](store, true, ctx,
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return store.buildTransactionQuery(filter.PITFilterWithVolumes, query).
				Where("transactions.id = ?", filter.ID).
				Limit(1)
		})
}
