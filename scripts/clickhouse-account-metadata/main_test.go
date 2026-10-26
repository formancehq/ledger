package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertPreservesNumbersAndMetadata(t *testing.T) {
	t.Parallel()

	const amount = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	data := `{"accountMetadata":{"users:001":"{\"name\":\"Alice\",\"empty\":\"\",\"escaped\":\"a\\\"b\\\\c\"}"},` +
		`"transaction":{"postings":[{"amount":18446744073709551617,"asset":"USD"}]},` +
		`"revertTransaction":{"postings":[{"amount":` + amount + `}]},` +
		`"unknown":{"amount":18446744073709551617,"fraction":1.234567890123456789e-12}}`
	var input, output bytes.Buffer
	for _, data := range []string{data, `{"other":18446744073709551617}`, `{"accountMetadata":{}}`} {
		require.NoError(t, json.NewEncoder(&input).Encode(map[string]any{
			"log_sequence": json.RawMessage(`18446744073709551615`), "data": data,
		}))
	}
	// Whitespace/line boundaries do not affect row framing.
	require.NoError(t, convert(&input, &output))
	decoder := json.NewDecoder(&output)
	var row map[string]json.RawMessage
	require.NoError(t, decoder.Decode(&row))
	require.Equal(t, `18446744073709551615`, string(row["log_sequence"]))
	actual, err := object(row["data"])
	require.NoError(t, err)
	original, err := object([]byte(data))
	require.NoError(t, err)
	require.Equal(t, original["unknown"], actual["unknown"], "untyped numbers are never quoted or rounded")
	require.JSONEq(t, `{"users:001":{"name":"Alice","empty":"","escaped":"a\"b\\c"}}`, string(actual["accountMetadata"]))
	require.Contains(t, string(actual["transaction"]), `"amount":"18446744073709551617"`)
	require.Contains(t, string(actual["revertTransaction"]), `"amount":"`+amount+`"`)
	require.NoError(t, decoder.Decode(&row))
	require.JSONEq(t, `{"other":18446744073709551617}`, string(row["data"]))
	require.NoError(t, decoder.Decode(&row))
	require.JSONEq(t, `{"accountMetadata":{}}`, string(row["data"]))
}

func TestConvertRejectsInvalidLegacyMetadata(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, data, failure string
	}{
		{"invalid JSON", `{"accountMetadata":{"a":"not-json"}}`, "invalid character"},
		{"array", `{"accountMetadata":{"a":"[]"}}`, "expected object"},
		{"null object", `{"accountMetadata":{"a":"null"}}`, "expected object"},
		{"number", `{"accountMetadata":{"a":"{\"n\":123}"}}`, "expected string"},
		{"null value", `{"accountMetadata":{"a":"{\"n\":null}"}}`, "expected string"},
		{"duplicate key", `{"accountMetadata":{"a":"{\"n\":\"x\",\"n\":\"y\"}"}}`, "duplicate key"},
		{"duplicate account", `{"accountMetadata":{"a":"{}","a":"{}"}}`, "duplicate key"},
		{"trailing input", `{"accountMetadata":{"a":"{}{}"}}`, "unexpected data"},
		{"non-object data", `[]`, "expected object"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var input, output bytes.Buffer
			require.NoError(t, json.NewEncoder(&input).Encode(map[string]string{"data": test.data}))
			require.ErrorContains(t, convert(&input, &output), test.failure)
			require.Empty(t, output.String())
		})
	}
}

func TestConvertRejectsIncompleteExport(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	require.ErrorContains(t, convert(strings.NewReader(`{"data":"{}"}`+"\n"+`{"data":`), &output), "row 2")
	// A caller must check the exit status before importing any partial output.
	require.JSONEq(t, `{"data":{}}`, strings.TrimSpace(output.String()))
}
