package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Each test in this file exercises the coercion path added for #249: the
// CLI sends every --param value as a StringValue because it doesn't know
// the target type. The server's extract* helpers parse strings into the
// declared scalar type at compile time. The native typed paths
// (Int64Value, Uint64Value, BoolValue) keep working unchanged so direct
// gRPC clients don't regress.

func TestExtractInt64_StringCoercion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr string
	}{
		{name: "positive", raw: "42", want: 42},
		{name: "zero", raw: "0", want: 0},
		{name: "negative", raw: "-7", want: -7},
		{name: "max int64", raw: "9223372036854775807", want: math.MaxInt64},
		{name: "min int64", raw: "-9223372036854775808", want: math.MinInt64},
		{name: "fractional rejected", raw: "4.2", wantErr: "cannot parse"},
		{name: "garbage rejected", raw: "not-a-number", wantErr: "cannot parse"},
		{name: "overflow rejected", raw: "9223372036854775808", wantErr: "cannot parse"},
		{
			name: "empty string rejected (was the #249 risk: '' parsing as 0)",
			raw:  "", wantErr: "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := map[string]*commonpb.ParameterValue{
				"p": {Value: &commonpb.ParameterValue_StringValue{StringValue: tt.raw}},
			}

			got, err := extractInt64(params, "p")
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExtractUint64_StringCoercion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    uint64
		wantErr string
	}{
		{name: "positive", raw: "42", want: 42},
		{name: "zero", raw: "0", want: 0},
		{name: "max uint64", raw: "18446744073709551615", want: math.MaxUint64},
		{name: "negative rejected", raw: "-1", wantErr: "cannot parse"},
		{name: "overflow rejected", raw: "18446744073709551616", wantErr: "cannot parse"},
		{name: "garbage rejected", raw: "abc", wantErr: "cannot parse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := map[string]*commonpb.ParameterValue{
				"p": {Value: &commonpb.ParameterValue_StringValue{StringValue: tt.raw}},
			}

			got, err := extractUint64(params, "p")
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestExtractBool_StringCoercion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    bool
		wantErr string
	}{
		{name: "true", raw: "true", want: true},
		{name: "false", raw: "false", want: false},
		{name: "1", raw: "1", want: true},
		{name: "0", raw: "0", want: false},
		{name: "True (capitalized — ParseBool accepts)", raw: "True", want: true},
		{name: "garbage rejected", raw: "yes-please", wantErr: "cannot parse"},
		{name: "empty rejected", raw: "", wantErr: "cannot parse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			params := map[string]*commonpb.ParameterValue{
				"p": {Value: &commonpb.ParameterValue_StringValue{StringValue: tt.raw}},
			}

			got, err := extractBool(params, "p")
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestExtract_NativeTypedPathsStillWork guards the contract that direct
// typed values from non-CLI clients (Int64Value, Uint64Value, BoolValue)
// continue to be accepted unchanged.
func TestExtract_NativeTypedPathsStillWork(t *testing.T) {
	t.Parallel()

	t.Run("int64 native", func(t *testing.T) {
		t.Parallel()

		params := map[string]*commonpb.ParameterValue{
			"p": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: -42}},
		}
		got, err := extractInt64(params, "p")
		require.NoError(t, err)
		require.Equal(t, int64(-42), got)
	})

	t.Run("uint64 native", func(t *testing.T) {
		t.Parallel()

		params := map[string]*commonpb.ParameterValue{
			"p": {Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 42}},
		}
		got, err := extractUint64(params, "p")
		require.NoError(t, err)
		require.Equal(t, uint64(42), got)
	})

	t.Run("bool native", func(t *testing.T) {
		t.Parallel()

		params := map[string]*commonpb.ParameterValue{
			"p": {Value: &commonpb.ParameterValue_BoolValue{BoolValue: true}},
		}
		got, err := extractBool(params, "p")
		require.NoError(t, err)
		require.True(t, got)
	})
}

// TestIssue249Repro is the exact regression scenario from the bug report.
// CLI used to infer the type of "0000...0" as int64(0), then the server
// rejected with "expected string value, got int64" because the prepared
// query declared the param as string. The new CLI always sends
// StringValue, and extractString returns it unchanged — bug fixed.
func TestIssue249Repro(t *testing.T) {
	t.Parallel()

	hash := "0000000000000000000000000000000000000000000000000000000000000000"
	params := map[string]*commonpb.ParameterValue{
		"hash": {Value: &commonpb.ParameterValue_StringValue{StringValue: hash}},
	}

	// The originally-failing case: prepared query expects string.
	got, err := extractString(params, "hash")
	require.NoError(t, err)
	require.Equal(t, hash, got)
}
