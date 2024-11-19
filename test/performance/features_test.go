//go:build it

package performance_test

import (
	. "github.com/formancehq/go-libs/v2/collectionutils"
	features2 "github.com/formancehq/ledger/pkg/features"
	"sort"
)

func buildAllPossibleConfigurations() []configuration {
	possibleConfigurations := make([]configuration, 0)
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "MINIMAL",
		FeatureSet: features2.MinimalFeatureSet,
	})

	fullConfiguration := features2.MinimalFeatureSet
	features := Keys(features2.FeatureConfigurations)
	sort.Strings(features)

	for _, feature := range features {
		possibleConfigurations = append(possibleConfigurations, configuration{
			Name:       feature,
			FeatureSet: features2.MinimalFeatureSet.With(feature, features2.FeatureConfigurations[feature][0]),
		})
		fullConfiguration = fullConfiguration.With(feature, features2.FeatureConfigurations[feature][0])
	}
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "FULL",
		FeatureSet: fullConfiguration,
	})

	return possibleConfigurations
}

type configuration struct {
	Name       string
	FeatureSet features2.FeatureSet
}

func (c configuration) String() string {
	return c.Name
}
