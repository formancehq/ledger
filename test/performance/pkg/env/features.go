//go:build it

package env

import (
	. "github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/ledger/pkg/features"
	"sort"
)

func BuildAllPossibleConfigurations() []Configuration {
	possibleConfigurations := make([]Configuration, 0)
	possibleConfigurations = append(possibleConfigurations, Configuration{
		Name:       "MINIMAL",
		FeatureSet: features.MinimalFeatureSet,
	})

	fullConfiguration := features.MinimalFeatureSet
	allFeatures := Keys(features.FeatureConfigurations)
	sort.Strings(allFeatures)

	for _, feature := range allFeatures {
		possibleConfigurations = append(possibleConfigurations, Configuration{
			Name:       feature,
			FeatureSet: features.MinimalFeatureSet.With(feature, features.FeatureConfigurations[feature][0]),
		})
		fullConfiguration = fullConfiguration.With(feature, features.FeatureConfigurations[feature][0])
	}
	possibleConfigurations = append(possibleConfigurations, Configuration{
		Name:       "FULL",
		FeatureSet: fullConfiguration,
	})

	return possibleConfigurations
}

type Configuration struct {
	Name       string
	FeatureSet features.FeatureSet
}

func (c Configuration) String() string {
	return c.Name
}
