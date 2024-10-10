//go:build it

package performance_test

import (
	. "github.com/formancehq/go-libs/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	"sort"
)

func buildAllPossibleConfigurations() []configuration {
	possibleConfigurations := make([]configuration, 0)
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "MINIMAL",
		FeatureSet: ledger.MinimalFeatureSet,
	})

	fullConfiguration := ledger.MinimalFeatureSet
	features := Keys(ledger.FeatureConfigurations)
	sort.Strings(features)

	for _, feature := range features {
		possibleConfigurations = append(possibleConfigurations, configuration{
			Name:       feature,
			FeatureSet: ledger.MinimalFeatureSet.With(feature, ledger.FeatureConfigurations[feature][0]),
		})
		fullConfiguration = fullConfiguration.With(feature, ledger.FeatureConfigurations[feature][0])
	}
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "FULL",
		FeatureSet: fullConfiguration,
	})

	return possibleConfigurations
}

type configuration struct {
	Name string `json:"name"`
	FeatureSet ledger.FeatureSet `json:"featureSet"`
}

func (c configuration) String() string {
	return c.Name
}
