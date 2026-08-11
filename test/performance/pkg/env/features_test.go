//go:build it

package env

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/features"
)

// TestBuildAllPossibleConfigurations guards the contract between the benchmark
// enumerator and features.FeatureConfigurations: the enumerator takes the first value
// of every entry as that feature's default, so an entry with no values panics here.
// Open-ended features (free-form values that cannot be enumerated) must therefore live
// in features.OpenEndedFeatures instead.
func TestBuildAllPossibleConfigurations(t *testing.T) {
	t.Parallel()

	for feature, values := range features.FeatureConfigurations {
		require.NotEmpty(t, values,
			"feature %q has no configurable values; open-ended features belong in OpenEndedFeatures", feature)
	}

	require.NotPanics(t, func() {
		// MINIMAL + one per enumerable feature + FULL.
		require.Len(t, BuildAllPossibleConfigurations(), len(features.FeatureConfigurations)+2)
	})

	for feature := range features.OpenEndedFeatures {
		require.NotContains(t, features.FeatureConfigurations, feature,
			"open-ended feature %q must stay out of the enumerable map", feature)
	}
}
