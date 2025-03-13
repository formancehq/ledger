//go:build it

package performance_test

import (
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/pkg/features"
	"sort"
)

func buildAllPossibleConfigurations() []configuration {
	possibleConfigurations := make([]configuration, 0)
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "MINIMAL",
		FeatureSet: features.MinimalFeatureSet,
	})

	fullConfiguration := features.MinimalFeatureSet
	allFeatures := Keys(features.FeatureConfigurations)
	sort.Strings(allFeatures)

	for _, feature := range allFeatures {
		possibleConfigurations = append(possibleConfigurations, configuration{
			Name:       feature,
			FeatureSet: features.MinimalFeatureSet.With(feature, features.FeatureConfigurations[feature][0]),
		})
		fullConfiguration = fullConfiguration.With(feature, features.FeatureConfigurations[feature][0])
	}
	possibleConfigurations = append(possibleConfigurations, configuration{
		Name:       "FULL",
		FeatureSet: fullConfiguration,
	})

	return possibleConfigurations
}

type configuration struct {
	Name       string
	FeatureSet features.FeatureSet
}

func (c configuration) String() string {
	return c.Name
}
