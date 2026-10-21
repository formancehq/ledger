package queries

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONNumberToInt(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]string{
		"123":                  "123",
		"-42":                  "-42",
		"123.0":                "123",
		"1e3":                  "1000",
		"1E+3":                 "1000",
		"1.5e1":                "15",
		"12345678901234567890": "12345678901234567890",
	} {
		x, err := jsonNumberToInt(json.Number(raw))
		require.NoError(t, err, raw)
		require.Equal(t, want, x.String(), raw)
	}

	for _, raw := range []string{"1.5", "1e-3", "1e1001", "1e-1001", "abc"} {
		_, err := jsonNumberToInt(json.Number(raw))
		require.Error(t, err, raw)
	}
}
