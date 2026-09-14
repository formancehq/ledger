package commonpb

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataValueFromJSONNumber(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		input string
		want  *MetadataValue
	}{
		{"0", NewUintValue(0)},
		{"-0", NewUintValue(0)},
		{"42", NewUintValue(42)},
		{"-42", NewIntValue(-42)},
		{"9007199254740991", NewUintValue(9007199254740991)},
		{"9007199254740992", NewUintValue(9007199254740992)},
		{"9007199254740993", NewUintValue(9007199254740993)},
		{"-9007199254740991", NewIntValue(-9007199254740991)},
		{"-9007199254740992", NewIntValue(-9007199254740992)},
		{"-9007199254740993", NewIntValue(-9007199254740993)},
		{"9223372036854775807", NewUintValue(math.MaxInt64)},
		{"9223372036854775808", NewUintValue(uint64(1) << 63)},
		{"-9223372036854775808", NewIntValue(math.MinInt64)},
		{"18446744073709551615", NewUintValue(math.MaxUint64)},
		{"1.0", NewUintValue(1)},
		{"1e3", NewUintValue(1000)},
		{"10e-1", NewUintValue(1)},
		{"0.0100e+4", NewUintValue(100)},
		{"-1.2E+1", NewIntValue(-12)},
		{"9007199254740993.000", NewUintValue(9007199254740993)},
		{"9.007199254740993e15", NewUintValue(9007199254740993)},
		{"-9.223372036854775808e18", NewIntValue(math.MinInt64)},
		{"184467440737095516150e-1", NewUintValue(math.MaxUint64)},
		{"0e999999999999999999999999", NewUintValue(0)},
		{"-0.000e-999999999999999999999999", NewUintValue(0)},
		{"1" + strings.Repeat("0", 1000) + "e-1000", NewUintValue(1)},
	} {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			got, err := MetadataValueFromAny(json.Number(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataValueFromJSONNumberRejectsUnsupportedValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		input string
		error string
	}{
		{"1.5", "float values are not supported"},
		{"-1.5", "float values are not supported"},
		{"9007199254740993.1", "float values are not supported"},
		{"-9007199254740993.1", "float values are not supported"},
		{"1.000000000000000000000000001", "float values are not supported"},
		{"1e-999999999999999999999999", "float values are not supported"},
		{"-1e-999999999999999999999999", "float values are not supported"},
		{"1e-999999", "float values are not supported"},
		{"1e-100", "float values are not supported"},
		{"100e-3", "float values are not supported"},
		{"18446744073709551616", "overflows uint64"},
		{"184467440737095516160e-1", "overflows uint64"},
		{"-9223372036854775809", "overflows int64"},
		{"-9223372036854775809.0", "overflows int64"},
		{"1e999999999999999999999999", "overflows uint64"},
		{"-1e999999999999999999999999", "overflows int64"},
		{"1e999999", "overflows uint64"},
		{"1e100", "overflows uint64"},
		{"", "invalid metadata number"},
		{"null", "invalid metadata number"},
		{"[]", "invalid metadata number"},
		{"1 ", "invalid metadata number"},
		{"01", "invalid metadata number"},
		{"1e", "invalid metadata number"},
		{"1.2.3", "invalid metadata number"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()

			got, err := MetadataValueFromAny(json.Number(tc.input))
			require.ErrorContains(t, err, tc.error)
			require.Nil(t, got)
		})
	}
}

func TestMetadataJSONDecodersPreserveNumbers(t *testing.T) {
	t.Parallel()

	for _, decoder := range []struct {
		name   string
		decode func(string) (map[string]*MetadataValue, error)
	}{
		{"metadata map", func(number string) (map[string]*MetadataValue, error) {
			var value MetadataMap
			err := json.Unmarshal([]byte(`{"value":`+number+`}`), &value)

			return value.GetValues(), err
		}},
		{"saved metadata", func(number string) (map[string]*MetadataValue, error) {
			var value SavedMetadata
			err := json.Unmarshal([]byte(`{"targetType":"ACCOUNT","targetId":"users:001","metadata":{"value":`+number+`}}`), &value)

			return value.GetMetadata(), err
		}},
	} {
		t.Run(decoder.name, func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				number string
				want   *MetadataValue
				error  string
			}{
				{"42", NewUintValue(42), ""},
				{"9007199254740993", NewUintValue(9007199254740993), ""},
				{"-9007199254740993", NewIntValue(-9007199254740993), ""},
				{"9223372036854775807", NewUintValue(math.MaxInt64), ""},
				{"-9223372036854775808", NewIntValue(math.MinInt64), ""},
				{"18446744073709551615", NewUintValue(math.MaxUint64), ""},
				{"9.007199254740993e15", NewUintValue(9007199254740993), ""},
				{"1.5", nil, "float values are not supported"},
				{"9007199254740993.1", nil, "float values are not supported"},
				{"1e-1000", nil, "float values are not supported"},
				{"-9223372036854775809", nil, "overflows int64"},
				{"18446744073709551616", nil, "overflows uint64"},
			} {
				t.Run(tc.number, func(t *testing.T) {
					t.Parallel()

					got, err := decoder.decode(tc.number)
					if tc.error != "" {
						require.ErrorContains(t, err, tc.error)

						return
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, got["value"])
				})
			}
		})
	}
}
