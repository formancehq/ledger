package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
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
		params  map[string]*ledgerpb.ParameterValue
		wantErr string
	}{
		{"missing parameter", map[string]*ledgerpb.ParameterValue{}, `parameter "p0" not provided`},
		{
			"wrongly typed parameter",
			map[string]*ledgerpb.ParameterValue{param: {Value: &ledgerpb.ParameterValue_StringValue{StringValue: "not-a-number"}}},
			`cannot parse "not-a-number"`,
		},
	}

	t.Run("uint", func(t *testing.T) {
		t.Parallel()

		lo := uint64(math.MaxUint64)
		cond := &ledgerpb.UintCondition{Min: &lo, MinExclusive: true, ParamMax: param}

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

			b, err := resolveUintBounds(cond, map[string]*ledgerpb.ParameterValue{
				param: {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: 10}},
			})
			require.NoError(t, err)
			require.True(t, b.empty)
		})
	})

	t.Run("int", func(t *testing.T) {
		t.Parallel()

		lo := int64(math.MaxInt64)
		cond := &ledgerpb.IntCondition{Min: &lo, MinExclusive: true, ParamMax: param}

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

			b, err := resolveIntBounds(cond, map[string]*ledgerpb.ParameterValue{
				param: {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: 10}},
			})
			require.NoError(t, err)
			require.True(t, b.empty)
		})
	})

	// The impossible min may itself come from a parameter.
	t.Run("uint parameterised impossible min", func(t *testing.T) {
		t.Parallel()

		cond := &ledgerpb.UintCondition{ParamMin: "lo", MinExclusive: true, ParamMax: param}
		params := map[string]*ledgerpb.ParameterValue{
			"lo": {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: math.MaxUint64}},
		}

		_, err := resolveUintBounds(cond, params)
		require.ErrorContains(t, err, `parameter "p0" not provided`)

		params[param] = &ledgerpb.ParameterValue{Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: 10}}
		b, err := resolveUintBounds(cond, params)
		require.NoError(t, err)
		require.True(t, b.empty)
	})
}

// Parameter bounds take precedence over literal bounds on each side, and
// exclusivity is applied after the parameter has been resolved.
func TestResolveBounds_ParameterPrecedenceAndExclusivity(t *testing.T) {
	t.Parallel()

	t.Run("int64", func(t *testing.T) {
		t.Parallel()
		literalMin, literalMax := int64(100), int64(200)
		b, err := resolveIntBounds(&ledgerpb.IntCondition{
			Min: &literalMin, Max: &literalMax, ParamMin: "lo", ParamMax: "hi",
			MinExclusive: true, MaxExclusive: false,
		}, map[string]*ledgerpb.ParameterValue{
			"lo": {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: 24}},
			"hi": {Value: &ledgerpb.ParameterValue_Int64Value{Int64Value: 25}},
		})
		require.NoError(t, err)
		require.Equal(t, resolvedIntBounds{min: 25, max: 26, hasMin: true, hasMax: true}, b)
		require.True(t, b.isEquality())
	})

	t.Run("uint64", func(t *testing.T) {
		t.Parallel()
		literalMin, literalMax := uint64(100), uint64(200)
		b, err := resolveUintBounds(&ledgerpb.UintCondition{
			Min: &literalMin, Max: &literalMax, ParamMin: "lo", ParamMax: "hi",
			MinExclusive: true, MaxExclusive: true,
		}, map[string]*ledgerpb.ParameterValue{
			"lo": {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: 24}},
			"hi": {Value: &ledgerpb.ParameterValue_Uint64Value{Uint64Value: 26}},
		})
		require.NoError(t, err)
		require.Equal(t, resolvedUintBounds{min: 25, max: 26, hasMin: true, hasMax: true}, b)
		require.True(t, b.isEquality())
	})
}
