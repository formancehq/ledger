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
			name: "account",
			validators: []propertyValidator{
				propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
					return validateAddressFilter(l, operator, value)
				}),
			},
		},
		{
			name: "address",
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
	}
}

func (h volumesResourceHandler) buildDataset(store *Store, ledger ledger.Ledger, opts ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions]) (*bun.SelectQuery, error) {

	var selectVolumes *bun.SelectQuery

	if (opts.PIT == nil || opts.PIT.IsZero()) && (opts.OOT == nil || opts.OOT.IsZero()) {
		selectVolumes = store.db.NewSelect().
			DistinctOn("accounts_address, asset").
			Column("asset", "input", "output").
			ColumnExpr("input - output as balance").
			ColumnExpr("accounts_address as account").
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			Where("ledger = ?", ledger.Name).
			Order("accounts_address", "asset")
	} else {
		if !ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
		}

		dateFilterColumn := "effective_date"
		if opts.Opts.UseInsertionDate {
			dateFilterColumn = "insertion_date"
		}

		selectVolumes = store.db.NewSelect().
			Column("asset").
			ColumnExpr("accounts_address as account").
			ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
			ColumnExpr("sum(case when is_source then amount else 0 end) as output").
			ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			Where("ledger = ?", ledger.Name).
			GroupExpr("accounts_address, asset").
			Order("accounts_address", "asset")

		if opts.PIT != nil && !opts.PIT.IsZero() {
			selectVolumes = selectVolumes.Where(dateFilterColumn+" <= ?", opts.PIT)
		}

		if opts.OOT != nil && !opts.OOT.IsZero() {
			selectVolumes = selectVolumes.Where(dateFilterColumn+" >= ?", opts.OOT)
		}
	}

	return selectVolumes, nil
}

func (h volumesResourceHandler) resolveFilter(
	store *Store,
	ledger ledger.Ledger,
	opts ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions],
	operator, property string,
	value any,
) (string, []any, error) {

	switch {
	case property == "address" || property == "account":
		address := value.(string)
		if isPartialAddress(address) {
			return store.db.NewSelect().
				TableExpr(store.GetPrefixedRelationName("accounts")).
				ColumnExpr("true").
				Where(filterAccountAddress(address, "address")).
				Where("address = dataset.account").
				String(), []any{}, nil
		}

		return "account = ?", []any{address}, nil
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
		var selectMetadata *bun.SelectQuery
		if ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
			selectMetadata = store.db.NewSelect().
				DistinctOn("accounts_address").
				ModelTableExpr(store.GetPrefixedRelationName("accounts_metadata")).
				Where("accounts_address = dataset.account").
				Order("accounts_address", "revision desc")

			if opts.PIT != nil && !opts.PIT.IsZero() {
				selectMetadata = selectMetadata.Where("date <= ?", opts.PIT)
			}
		} else {
			selectMetadata = store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Where("address = dataset.account")
		}
		selectMetadata = selectMetadata.
			Where("ledger = ?", ledger.Name).
			Column("metadata").
			Limit(1)

		switch {
		case metadataRegex.Match([]byte(property)):
			match := metadataRegex.FindAllStringSubmatch(property, 3)

			return "(?) @> ?", []any{selectMetadata, map[string]any{
				match[0][1]: value,
			}}, nil

		case property == "metadata":
			return "(?) -> ? is not null", []any{selectMetadata, value}, nil
		default:
			panic("unreachable")
		}
	}

	panic("unreachable")
}

func (h volumesResourceHandler) aggregate(
	store *Store,
	ledger ledger.Ledger,
	query ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	if query.Opts.GroupLvl == 0 {
		return selectQuery, nil
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

func (h volumesResourceHandler) expand(_ *Store, _ ledger.Ledger, _ ledgercontroller.ResourceQuery[ledgercontroller.GetVolumesOptions], property string) (*bun.SelectQuery, *joinCondition, error) {
	return nil, nil, errors.New("no expansion available")
}

var _ repositoryHandler[ledgercontroller.GetVolumesOptions] = volumesResourceHandler{}
