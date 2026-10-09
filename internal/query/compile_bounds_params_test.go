package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestResolveBounds_ImpossibleSideStillValidatesParams pins that a bound
// which is impossible on its own (`field > MaxUint64`) does not skip the
// resolution of the parameter on the other side. Whether a missing or
// mistyped parameter is refused must not depend on the other bound's value
// (Antithesis run d0973bd5d253d277a67f16526c2d9525-61-9).
func TestResolveBounds_ImpossibleSideStillValidatesParams(t *testing.T) {
	t.Parallel()

	const param = "p0"

	paramCases := []struct {
		name    string
		params  map[string]*commonpb.ParameterValue
		wantErr string
	}{
		{"missing parameter", map[string]*commonpb.ParameterValue{}, `parameter "p0" not provided`},
		{
			"wrongly typed parameter",
			map[string]*commonpb.ParameterValue{param: {Value: &commonpb.ParameterValue_StringValue{StringValue: "not-a-number"}}},
			`cannot parse "not-a-number"`,
		},
	}

	t.Run("uint", func(t *testing.T) {
		t.Parallel()

		lo := uint64(math.MaxUint64)
		cond := &commonpb.UintCondition{Min: &lo, MinExclusive: true, ParamMax: param}

		for _, tc := range paramCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				_, err := resolveUintBounds(cond, tc.params)
				var be *domain.BusinessError
				require.ErrorAs(t, err, &be)
				require.ErrorContains(t, err, tc.wantErr)
			})
		}

		t.Run("valid parameter is the empty match", func(t *testing.T) {
			t.Parallel()

			b, err := resolveUintBounds(cond, map[string]*commonpb.ParameterValue{
				param: {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 10}},
			})
			require.NoError(t, err)
			require.True(t, b.empty)
		})
	})

	t.Run("int", func(t *testing.T) {
		t.Parallel()

		lo := int64(math.MaxInt64)
		cond := &commonpb.IntCondition{Min: &lo, MinExclusive: true, ParamMax: param}

		for _, tc := range paramCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				_, err := resolveIntBounds(cond, tc.params)
				var be *domain.BusinessError
				require.ErrorAs(t, err, &be)
				require.ErrorContains(t, err, tc.wantErr)
			})
		}

		t.Run("valid parameter is the empty match", func(t *testing.T) {
			t.Parallel()

			b, err := resolveIntBounds(cond, map[string]*commonpb.ParameterValue{
				param: {Value: &commonpb.ParameterValue_Int64Value{Int64Value: 10}},
			})
			require.NoError(t, err)
			require.True(t, b.empty)
		})
	})

	// The impossible min may itself come from a parameter.
	t.Run("uint parameterised impossible min", func(t *testing.T) {
		t.Parallel()

		cond := &commonpb.UintCondition{ParamMin: "lo", MinExclusive: true, ParamMax: param}
		params := map[string]*commonpb.ParameterValue{
			"lo": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: math.MaxUint64}},
		}

		_, err := resolveUintBounds(cond, params)
		require.ErrorContains(t, err, `parameter "p0" not provided`)

		params[param] = &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 10}}
		b, err := resolveUintBounds(cond, params)
		require.NoError(t, err)
		require.True(t, b.empty)
	})
}
