//go:build it

package performance_test

import (
	. "github.com/formancehq/go-libs/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	"slices"
	"strings"
)

func buildAllPossibleConfigurations() []FeatureConfiguration {
	possibleConfigurations := make([]FeatureConfiguration, 0)

	for _, feature := range Keys(ledger.FeatureConfigurations) {
		optionsForFeature := Map(ledger.FeatureConfigurations[feature], func(from string) FeatureConfiguration {
			return FeatureConfiguration{
				feature: from,
			}
		})
		if len(possibleConfigurations) == 0 {
			possibleConfigurations = append(possibleConfigurations, optionsForFeature...)
		} else {
			newPossibleConfigurations := make([]FeatureConfiguration, 0)
			for _, existing := range possibleConfigurations {
				for _, optionForFeature := range optionsForFeature {
					for k, v := range optionForFeature {
						replaced := make(map[string]string)
						for k, v := range existing {
							replaced[k] = v
						}
						replaced[k] = v
						newPossibleConfigurations = append(newPossibleConfigurations, replaced)
					}
				}
			}
			possibleConfigurations = newPossibleConfigurations
		}
	}

	return possibleConfigurations
}

type FeatureConfiguration map[string]string

func (cfg FeatureConfiguration) String() string {
	if len(cfg) == 0 {
		return ""
	}
	keys := Keys(cfg)
	slices.Sort(keys)

	ret := ""
	for _, key := range keys {
		ret = ret + "," + shortenFeature(key) + "=" + cfg[key]
	}

	return ret[1:]
}

func shortenFeature(feature string) string {
	return strings.Join(Map(strings.Split(feature, "_"), func(from string) string {
		return from[:1]
	}), "")
}
