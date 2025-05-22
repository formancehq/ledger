package ledger

import (
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"strings"
)

type volumesResourceHandler struct {
	store *Store
}

func (h volumesResourceHandler) Filters() []common.Filter {
	return []common.Filter{
		{
			Name:    "address",
			Aliases: []string{"account"},
			Validators: []common.PropertyValidator{
				common.PropertyValidatorFunc(func(operator string, key string, value any) error {
					return validateAddressFilter(operator, value)
				}),
			},
		},
		{
			Name: `balance(\[.*])?`,
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			Name: "first_usage",
			Validators: []common.PropertyValidator{
				common.AcceptOperators("$lt", "$gt", "$lte", "$gte", "$match"),
			},
		},
		{
			Name: "metadata",
			Matchers: []func(string) bool{
				func(key string) bool {
					return key == "metadata" || common.MetadataRegex.Match([]byte(key))
				},
			},
			Validators: []common.PropertyValidator{
				common.PropertyValidatorFunc(func(operator string, key string, value any) error {
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

func (h volumesResourceHandler) BuildDataset(query common.RepositoryHandlerBuildContext[ledgercontroller.GetVolumesOptions]) (*bun.SelectQuery, error) {

	var selectVolumes *bun.SelectQuery

	needAddressSegments := query.UseFilter("address", isPartialAddress)
	if !query.UsePIT() && !query.UseOOT() {
		selectVolumes = h.store.db.NewSelect().
			Column("asset", "input", "output").
			ColumnExpr("input - output as balance").
			ColumnExpr("accounts_address as account").
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Where("ledger = ?", h.store.ledger.Name).
			Order("accounts_address", "asset")

		if query.UseFilter("metadata") || query.UseFilter("first_usage") || needAddressSegments {
			accountsQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("ledger = ?", h.store.ledger.Name).
				Where("accounts.address = accounts_address")

			if needAddressSegments {
				accountsQuery = accountsQuery.ColumnExpr("address_array as account_array")
				selectVolumes = selectVolumes.Column("account_array")
			}
			if query.UseFilter("metadata") {
				accountsQuery = accountsQuery.ColumnExpr("metadata")
				selectVolumes = selectVolumes.Column("metadata")
			}
			if query.UseFilter("first_usage") {
				accountsQuery = accountsQuery.Column("first_usage")
				selectVolumes = selectVolumes.Column("first_usage")
			}

			selectVolumes = selectVolumes.
				Join(`join lateral (?) accounts on true`, accountsQuery)
		}
	} else {
		if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
		}

		selectVolumes = h.store.db.NewSelect().
			Column("asset").
			ColumnExpr("accounts_address as account").
			ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
			ColumnExpr("sum(case when is_source then amount else 0 end) as output").
			ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			Where("ledger = ?", h.store.ledger.Name).
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

		if needAddressSegments || query.UseFilter("first_usage") {
			accountsQuery := h.store.db.NewSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Where("accounts.address = accounts_address").
				Where("ledger = ?", h.store.ledger.Name)

			if needAddressSegments {
				accountsQuery = accountsQuery.ColumnExpr("address_array")
				selectVolumes = selectVolumes.ColumnExpr("(array_agg(accounts.address_array))[1] as account_array")
			}
			if query.UseFilter("first_usage") {
				accountsQuery = accountsQuery.ColumnExpr("first_usage")
				selectVolumes = selectVolumes.ColumnExpr("(array_agg(accounts.first_usage))[1] as first_usage")
			}
			selectVolumes = selectVolumes.Join(`join lateral (?) accounts on true`, accountsQuery)
		}

		if query.UseFilter("metadata") {
			subQuery := h.store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
				ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
				Where("ledger = ?", h.store.ledger.Name).
				Where("accounts_metadata.accounts_address = moves.accounts_address")

			selectVolumes = selectVolumes.
				Join(`left join lateral (?) accounts_metadata on true`, subQuery).
				ColumnExpr("(array_agg(metadata))[1] as metadata")
		}
	}

	return selectVolumes, nil
}

func (h volumesResourceHandler) ResolveFilter(
	_ common.ResourceQuery[ledgercontroller.GetVolumesOptions],
	operator, property string,
	value any,
) (string, []any, error) {

	switch {
	case property == "address" || property == "account":
		return filterAccountAddress(value.(string), "account"), nil, nil
	case property == "first_usage":
		return fmt.Sprintf("first_usage %s ?", common.ConvertOperatorToSQL(operator)), []any{value}, nil
	case balanceRegex.MatchString(property) || property == "balance":
		clauses := make([]string, 0)
		args := make([]any, 0)

		clauses = append(clauses, "balance "+common.ConvertOperatorToSQL(operator)+" ?")
		args = append(args, value)

		if balanceRegex.MatchString(property) {
			clauses = append(clauses, "asset = ?")
			args = append(args, balanceRegex.FindAllStringSubmatch(property, 2)[0][1])
		}

		return "(" + strings.Join(clauses, ") and (") + ")", args, nil
	case common.MetadataRegex.Match([]byte(property)) || property == "metadata":
		if property == "metadata" {
			return "metadata -> ? is not null", []any{value}, nil
		} else {
			match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

			return "metadata @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		}
	default:
		return "", nil, fmt.Errorf("unsupported filter %s", property)
	}
}

func (h volumesResourceHandler) Project(
	query common.ResourceQuery[ledgercontroller.GetVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	selectQuery = selectQuery.DistinctOn("account, asset")

	if query.Opts.GroupLvl == 0 {
		return selectQuery.ColumnExpr("*"), nil
	}

	intermediate := h.store.db.NewSelect().
		ModelTableExpr("(?) data", selectQuery).
		Column("asset", "input", "output", "balance").
		ColumnExpr(fmt.Sprintf(`(array_to_string((string_to_array(account, ':'))[1:LEAST(array_length(string_to_array(account, ':'),1),%d)],':')) as account`, query.Opts.GroupLvl))

	return h.store.db.NewSelect().
		ModelTableExpr("(?) data", intermediate).
		Column("account", "asset").
		ColumnExpr("sum(input) as input").
		ColumnExpr("sum(output) as output").
		ColumnExpr("sum(balance) as balance").
		GroupExpr("account, asset"), nil
}

func (h volumesResourceHandler) Expand(_ common.ResourceQuery[ledgercontroller.GetVolumesOptions], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ common.RepositoryHandler[ledgercontroller.GetVolumesOptions] = volumesResourceHandler{}
