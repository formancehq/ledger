package ledger

import (
	"fmt"
	"github.com/formancehq/ledger/internal/storage/resources"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"slices"
)

type transactionsResourceHandler struct{
	store *Store
}

func (h transactionsResourceHandler) Filters() []resources.Filter {
	return []resources.Filter{
		{
			Name: "reverted",
			Validators: []resources.PropertyValidator{
				resources.AcceptOperators("$match"),
			},
		},
		{
			Name: "account",
			Validators: []resources.PropertyValidator{
				resources.PropertyValidatorFunc(func(operator string, key string, value any) error {
					return validateAddressFilter(h.store.ledger, operator, value)
				}),
			},
		},
		{
			Name: "source",
			Validators: []resources.PropertyValidator{
				resources.PropertyValidatorFunc(func(operator string, key string, value any) error {
					return validateAddressFilter(h.store.ledger, operator, value)
				}),
			},
		},
		{
			Name: "destination",
			Validators: []resources.PropertyValidator{
				resources.PropertyValidatorFunc(func(operator string, key string, value any) error {
					return validateAddressFilter(h.store.ledger, operator, value)
				}),
			},
		},
		{
			// todo: add validators
			Name: "timestamp",
		},
		{
			Name: "metadata",
			Validators: []resources.PropertyValidator{
				resources.AcceptOperators("$exists"),
			},
		},
		{
			Name: `metadata\[.*]`,
			Validators: []resources.PropertyValidator{
				resources.AcceptOperators("$match"),
			},
		},
		{
			Name: "id",
		},
		{
			Name: "reference",
		},
	}
}

func (h transactionsResourceHandler) BuildDataset(opts resources.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := h.store.db.NewSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("transactions")).
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
		Where("ledger = ?", h.store.ledger.Name)

	if slices.Contains(opts.Expand, "volumes") {
		ret = ret.Column("post_commit_volumes")
	}

	if opts.PIT != nil && !opts.PIT.IsZero() {
		ret = ret.Where("timestamp <= ?", opts.PIT)
	}

	if h.store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
		selectDistinctTransactionMetadataHistories := h.store.db.NewSelect().
			DistinctOn("transactions_id").
			ModelTableExpr(h.store.GetPrefixedRelationName("transactions_metadata")).
			Where("ledger = ?", h.store.ledger.Name).
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

func (h transactionsResourceHandler) ResolveFilter(opts resources.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "id":
		return fmt.Sprintf("id %s ?", resources.ConvertOperatorToSQL(operator)), []any{value}, nil
	case property == "reference" || property == "timestamp":
		return fmt.Sprintf("%s %s ?", property, resources.ConvertOperatorToSQL(operator)), []any{value}, nil
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

func (h transactionsResourceHandler) Project(query resources.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h transactionsResourceHandler) Expand(opts resources.ResourceQuery[any], property string) (*bun.SelectQuery, *resources.JoinCondition, error) {
	if property != "effectiveVolumes" {
		return nil, nil, nil
	}

	ret := h.store.db.NewSelect().
		TableExpr(
			"(?) data",
		h.store.db.NewSelect().
				TableExpr(
					"(?) moves",
					h.store.db.NewSelect().
						DistinctOn("transactions_id, accounts_address, asset").
						ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
						Column("transactions_id", "accounts_address", "asset").
						ColumnExpr(`first_value(moves.post_commit_effective_volumes) over (partition by (transactions_id, accounts_address, asset) order by seq desc) as post_commit_effective_volumes`).
						Where("ledger = ?", h.store.ledger.Name).
						Where("transactions_id in (select id from dataset)"),
				).
				Column("transactions_id", "accounts_address").
				ColumnExpr(`public.aggregate_objects(json_build_object(moves.asset, json_build_object('input', (moves.post_commit_effective_volumes).inputs, 'output', (moves.post_commit_effective_volumes).outputs))::jsonb) AS post_commit_effective_volumes`).
				Group("transactions_id", "accounts_address"),
		).
		Column("transactions_id").
		ColumnExpr("public.aggregate_objects(json_build_object(accounts_address, post_commit_effective_volumes)::jsonb) AS post_commit_effective_volumes").
		Group("transactions_id")

	return ret, &resources.JoinCondition{
		Left:  "id",
		Right: "transactions_id",
	}, nil
}

var _ resources.RepositoryHandler[any] = transactionsResourceHandler{}
