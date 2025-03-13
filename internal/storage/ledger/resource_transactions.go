package ledger

import (
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"slices"
)

type transactionsResourceHandler struct{}

func (h transactionsResourceHandler) filters() []filter {
	return []filter{
		{
			name: "reverted",
			validators: []propertyValidator{
				acceptOperators("$match"),
			},
		},
		{
			name: "account",
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			name: "source",
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			name: "destination",
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			// todo: add validators
			name: "timestamp",
		},
		{
			name: "metadata",
			validators: []propertyValidator{
				acceptOperators("$exists"),
			},
		},
		{
			name: `metadata\[.*]`,
			validators: []propertyValidator{
				acceptOperators("$match"),
			},
		},
		{
			name: "id",
		},
		{
			name: "reference",
		},
	}
}

func (h transactionsResourceHandler) buildDataset(store *Store, opts repositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := store.db.NewSelect().
		ModelTableExpr(store.GetPrefixedRelationName("transactions")).
		Column(
			"ledger",
			"id",
			"timestamp",
			"reference",
			"inserted_at",
			"updated_at",
			"postings",
			"sources",
			"destinations",
			"sources_arrays",
			"destinations_arrays",
		).
		Where("ledger = ?", store.ledger.Name)

	if slices.Contains(opts.Expand, "volumes") {
		ret = ret.Column("post_commit_volumes")
	}

	if opts.PIT != nil && !opts.PIT.IsZero() {
		ret = ret.Where("timestamp <= ?", opts.PIT)
	}

	if store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
		selectDistinctTransactionMetadataHistories := store.db.NewSelect().
			DistinctOn("transactions_id").
			ModelTableExpr(store.GetPrefixedRelationName("transactions_metadata")).
			Where("ledger = ?", store.ledger.Name).
			Column("transactions_id", "metadata").
			Order("transactions_id", "revision desc").
			Where("date <= ?", opts.PIT)

		ret = ret.
			Join(
				`left join (?) transactions_metadata on transactions_metadata.transactions_id = transactions.id`,
				selectDistinctTransactionMetadataHistories,
			).
			ColumnExpr("coalesce(transactions_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("metadata")
	}

	if opts.UsePIT() {
		ret = ret.ColumnExpr("(case when transactions.reverted_at <= ? then transactions.reverted_at else null end) as reverted_at", opts.PIT)
	} else {
		ret = ret.Column("reverted_at")
	}

	return ret, nil
}

func (h transactionsResourceHandler) resolveFilter(store *Store, opts ledgercontroller.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "id":
		return fmt.Sprintf("id %s ?", convertOperatorToSQL(operator)), []any{value}, nil
	case property == "reference" || property == "timestamp":
		return fmt.Sprintf("%s %s ?", property, convertOperatorToSQL(operator)), []any{value}, nil
	case property == "reverted":
		ret := "reverted_at is"
		if value.(bool) {
			ret += " not"
		}
		return ret + " null", nil, nil
	case property == "account":
		return filterAccountAddressOnTransactions(value.(string), true, true), nil, nil
	case property == "source":
		return filterAccountAddressOnTransactions(value.(string), true, false), nil, nil
	case property == "destination":
		return filterAccountAddressOnTransactions(value.(string), false, true), nil, nil
	case metadataRegex.Match([]byte(property)):
		match := metadataRegex.FindAllStringSubmatch(property, 3)

		return "metadata @> ?", []any{map[string]any{
			match[0][1]: value,
		}}, nil

	case property == "metadata":
		return "metadata -> ? is not null", []any{value}, nil
	default:
		return "", nil, fmt.Errorf("unsupported filter: %s", property)
	}
}

func (h transactionsResourceHandler) project(store *Store, query ledgercontroller.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h transactionsResourceHandler) expand(store *Store, opts ledgercontroller.ResourceQuery[any], property string) (*bun.SelectQuery, *joinCondition, error) {
	if property != "effectiveVolumes" {
		return nil, nil, nil
	}

	ret := store.db.NewSelect().
		TableExpr(
			"(?) data",
			store.db.NewSelect().
				TableExpr(
					"(?) moves",
					store.db.NewSelect().
						DistinctOn("transactions_id, accounts_address, asset").
						ModelTableExpr(store.GetPrefixedRelationName("moves")).
						Column("transactions_id", "accounts_address", "asset").
						ColumnExpr(`first_value(moves.post_commit_effective_volumes) over (partition by (transactions_id, accounts_address, asset) order by seq desc) as post_commit_effective_volumes`).
						Where("ledger = ?", store.ledger.Name).
						Where("transactions_id in (select id from dataset)"),
				).
				Column("transactions_id", "accounts_address").
				ColumnExpr(`public.aggregate_objects(json_build_object(moves.asset, json_build_object('input', (moves.post_commit_effective_volumes).inputs, 'output', (moves.post_commit_effective_volumes).outputs))::jsonb) AS post_commit_effective_volumes`).
				Group("transactions_id", "accounts_address"),
		).
		Column("transactions_id").
		ColumnExpr("public.aggregate_objects(json_build_object(accounts_address, post_commit_effective_volumes)::jsonb) AS post_commit_effective_volumes").
		Group("transactions_id")

	return ret, &joinCondition{
		left:  "id",
		right: "transactions_id",
	}, nil
}

var _ repositoryHandler[any] = transactionsResourceHandler{}
