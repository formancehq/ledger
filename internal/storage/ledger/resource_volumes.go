package ledger

import (
	"errors"
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"strings"
)

type volumesResourceHandler struct{}

func (h volumesResourceHandler) filters() []filter {
	return []filter{
		{
			name:    "address",
			aliases: []string{"account"},
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			name: `balance(\[.*])?`,
			validators: []propertyValidator{
				acceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			name: "first_usage",
			validators: []propertyValidator{
				acceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			name: "metadata",
			matchers: []func(string) bool{
				func(key string) bool {
					return key == "metadata" || metadataRegex.Match([]byte(key))
				},
			},
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					if key == "metadata" {
						if operator != "$exists" {
							return fmt.Errorf("unsupported operator %s for metadata", operator)
						}
						return nil
					}
					if operator != "$match" {
						return fmt.Errorf("unsupported operator %s for metadata", operator)
					}
					return nil
				}),
			},
		},
	}
}

func (h volumesResourceHandler) buildDataset(store *Store, query repositoryHandlerBuildContext[ledgercontroller.GetVolumesOptions]) (*bun.SelectQuery, error) {

	var selectVolumes *bun.SelectQuery

	needAddressSegments := query.useFilter("address", isPartialAddress)
	if !query.UsePIT() && !query.UseOOT() {
		selectVolumes = store.db.NewSelect().
			Column("asset", "input", "output").
			ColumnExpr("input - output as balance").
			ColumnExpr("accounts_address as account").
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Where("ledger = ?", store.ledger.Name).
			Order("accounts_address", "asset")

		if query.useFilter("metadata") || query.useFilter("first_usage") || needAddressSegments {
			accountsQuery := store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("ledger = ?", store.ledger.Name).
				Where("accounts.address = accounts_address")

			if needAddressSegments {
				accountsQuery = accountsQuery.ColumnExpr("address_array as account_array")
				selectVolumes = selectVolumes.Column("account_array")
			}
			if query.useFilter("metadata") {
				accountsQuery = accountsQuery.ColumnExpr("metadata")
				selectVolumes = selectVolumes.Column("metadata")
			}
			if query.useFilter("first_usage") {
				accountsQuery = accountsQuery.Column("first_usage")
				selectVolumes = selectVolumes.Column("first_usage")
			}

			selectVolumes = selectVolumes.
				Join(`join lateral (?) accounts on true`, accountsQuery)
		}
	} else {
		if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
		}

		selectVolumes = store.db.NewSelect().
			Column("asset").
			ColumnExpr("accounts_address as account").
			ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
			ColumnExpr("sum(case when is_source then amount else 0 end) as output").
			ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			Where("ledger = ?", store.ledger.Name).
			GroupExpr("accounts_address, asset").
			Order("accounts_address", "asset")

		dateFilterColumn := "effective_date"
		if query.Opts.UseInsertionDate {
			dateFilterColumn = "insertion_date"
		}

		if query.UsePIT() {
			selectVolumes = selectVolumes.Where(dateFilterColumn+" <= ?", query.PIT)
		}

		if query.UseOOT() {
			selectVolumes = selectVolumes.Where(dateFilterColumn+" >= ?", query.OOT)
		}

		if needAddressSegments || query.useFilter("first_usage") {
			accountsQuery := store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				Where("accounts.address = accounts_address").
				Where("ledger = ?", store.ledger.Name)

			if needAddressSegments {
				accountsQuery = accountsQuery.ColumnExpr("address_array")
				selectVolumes = selectVolumes.ColumnExpr("(array_agg(accounts.address_array))[1] as account_array")
			}
			if query.useFilter("first_usage") {
				accountsQuery = accountsQuery.ColumnExpr("first_usage")
				selectVolumes = selectVolumes.ColumnExpr("(array_agg(accounts.first_usage))[1] as first_usage")
			}
			selectVolumes = selectVolumes.Join(`join lateral (?) accounts on true`, accountsQuery)
		}

		if query.useFilter("metadata") {
			subQuery := store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
				ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
				Where("ledger = ?", store.ledger.Name).
				Where("accounts_metadata.accounts_address = moves.accounts_address")

			selectVolumes = selectVolumes.
				Join(`left join lateral (?) accounts_metadata on true`, subQuery).
				ColumnExpr("(array_agg(metadata))[1] as metadata")
		}
	}

	return selectVolumes, nil
}

func (h volumesResourceHandler) resolveFilter(
	_ *Store,
	_ ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions],
	operator, property string,
	value any,
) (string, []any, error) {

	switch {
	case property == "address" || property == "account":
		return filterAccountAddress(value.(string), "account"), nil, nil
	case property == "first_usage":
		return fmt.Sprintf("first_usage %s ?", convertOperatorToSQL(operator)), []any{value}, nil
	case balanceRegex.MatchString(property) || property == "balance":
		clauses := make([]string, 0)
		args := make([]any, 0)

		clauses = append(clauses, "balance "+convertOperatorToSQL(operator)+" ?")
		args = append(args, value)

		if balanceRegex.MatchString(property) {
			clauses = append(clauses, "asset = ?")
			args = append(args, balanceRegex.FindAllStringSubmatch(property, 2)[0][1])
		}

		return "(" + strings.Join(clauses, ") and (") + ")", args, nil
	case metadataRegex.Match([]byte(property)) || property == "metadata":
		if property == "metadata" {
			return "metadata -> ? is not null", []any{value}, nil
		} else {
			match := metadataRegex.FindAllStringSubmatch(property, 3)

			return "metadata @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		}
	default:
		return "", nil, fmt.Errorf("unsupported filter %s", property)
	}
}

func (h volumesResourceHandler) project(
	store *Store,
	query ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	selectQuery = selectQuery.DistinctOn("account, asset")

	if query.Opts.GroupLvl == 0 {
		return selectQuery.ColumnExpr("*"), nil
	}

	intermediate := store.db.NewSelect().
		ModelTableExpr("(?) data", selectQuery).
		Column("asset", "input", "output", "balance").
		ColumnExpr(fmt.Sprintf(`(array_to_string((string_to_array(account, ':'))[1:LEAST(array_length(string_to_array(account, ':'),1),%d)],':')) as account`, query.Opts.GroupLvl))

	return store.db.NewSelect().
		ModelTableExpr("(?) data", intermediate).
		Column("account", "asset").
		ColumnExpr("sum(input) as input").
		ColumnExpr("sum(output) as output").
		ColumnExpr("sum(balance) as balance").
		GroupExpr("account, asset"), nil
}

func (h volumesResourceHandler) expand(_ *Store, _ ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions], property string) (*bun.SelectQuery, *joinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ repositoryHandler[ledgercontroller.GetVolumesOptions] = volumesResourceHandler{}
