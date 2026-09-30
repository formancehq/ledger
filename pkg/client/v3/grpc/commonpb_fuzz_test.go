package grpc

import (
	"testing"
)

// FuzzUint256UnmarshalJSON fuzzes the Uint256 JSON decoder with arbitrary byte slices.
// It targets the decimal string parser (no quotes) looking for panics, hangs,
// or unexpected behavior on malformed input.
func FuzzUint256UnmarshalJSON(f *testing.F) {
	// Seed corpus: valid values, edge cases, and known tricky inputs.
	f.Add([]byte("0"))
	f.Add([]byte("1"))
	f.Add([]byte("42"))
	f.Add([]byte("1000000000"))
	f.Add([]byte("18446744073709551615"))                                                           // max uint64
	f.Add([]byte("115792089237316195423570985008687907853269984665640564039457584007913129639935")) // max uint256
	f.Add([]byte("-1"))
	f.Add([]byte(""))
	f.Add([]byte("abc"))
	f.Add([]byte("0x1"))
	f.Add([]byte("1e10"))
	f.Add([]byte("999999999999999999999999999999999999999999999999999999999999999999999999999999999999"))
	f.Add([]byte("00000000000000000000000000000000000000000000001"))
	f.Add([]byte(" 42"))
	f.Add([]byte("42 "))
	f.Add([]byte("\x00"))
	f.Add([]byte("1.5"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var u Uint256

		err := u.UnmarshalJSON(data)
		if err != nil {
			return // invalid input is fine, just must not panic
		}

		// If parsing succeeded, verify the round-trip: marshal back and
		// unmarshal again — the result must be identical.
		out, err := u.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON failed after successful UnmarshalJSON: %v", err)
		}

		var u2 Uint256

		err = u2.UnmarshalJSON(out)
		if err != nil {
			t.Fatalf("round-trip UnmarshalJSON failed: %v", err)
		}

		if u.GetV0() != u2.GetV0() || u.GetV1() != u2.GetV1() ||
			u.GetV2() != u2.GetV2() || u.GetV3() != u2.GetV3() {
			t.Fatalf("round-trip mismatch: input=%q → marshal=%q", data, out)
		}
	})
}

// FuzzMetadataMapUnmarshalJSON fuzzes the MetadataMap JSON decoder.
// MetadataMap accepts a flat JSON object with typed values and performs
// type inference (string, int, uint, bool, null). This targets the
// inference logic and polymorphic dispatch.
func FuzzMetadataMapUnmarshalJSON(f *testing.F) {
	// Seed corpus: valid JSON objects with various value types.
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"key": "value"}`))
	f.Add([]byte(`{"count": 42}`))
	f.Add([]byte(`{"negative": -1}`))
	f.Add([]byte(`{"active": true}`))
	f.Add([]byte(`{"active": false}`))
	f.Add([]byte(`{"cleared": null}`))
	f.Add([]byte(`{"a": "str", "b": 42, "c": true, "d": null}`))
	f.Add([]byte(`{"big": 18446744073709551615}`))
	f.Add([]byte(`{"neg_big": -9223372036854775808}`))
	f.Add([]byte(`{"zero": 0}`))
	f.Add([]byte(`{"float": 3.14}`))
	f.Add([]byte(`{"nested": {"a": 1}}`))
	f.Add([]byte(`{"arr": [1, 2, 3]}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`"just a string"`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var mm MetadataMap

		err := mm.UnmarshalJSON(data)
		if err != nil {
			return // invalid input is fine, just must not panic
		}

		// If parsing succeeded, verify the round-trip.
		out, err := mm.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON failed after successful UnmarshalJSON: %v", err)
		}

		var mm2 MetadataMap

		err = mm2.UnmarshalJSON(out)
		if err != nil {
			t.Fatalf("round-trip UnmarshalJSON failed on %q: %v", out, err)
		}

		// Verify same number of keys.
		if len(mm.GetValues()) != len(mm2.GetValues()) {
			t.Fatalf("round-trip key count mismatch: %d vs %d (input=%q, output=%q)",
				len(mm.GetValues()), len(mm2.GetValues()), data, out)
		}
	})
}

// FuzzTimestampUnmarshalJSON fuzzes the Timestamp JSON decoder.
// It targets the RFC3339 time parser for panics and round-trip consistency.
func FuzzTimestampUnmarshalJSON(f *testing.F) {
	// Seed corpus: valid and edge-case timestamps.
	f.Add([]byte(`"2024-01-01T00:00:00Z"`))
	f.Add([]byte(`"2024-12-31T23:59:59.999999Z"`))
	f.Add([]byte(`"2024-06-15T12:30:00+02:00"`))
	f.Add([]byte(`"2024-06-15T12:30:00-05:00"`))
	f.Add([]byte(`"1970-01-01T00:00:00Z"`))
	f.Add([]byte(`"2000-01-01T00:00:00.000000Z"`))
	f.Add([]byte(`""`))
	f.Add([]byte(`"not-a-date"`))
	f.Add([]byte(`"2024"`))
	f.Add([]byte(`"2024-01-01"`))
	f.Add([]byte(`42`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"9999-12-31T23:59:59Z"`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var ts Timestamp

		err := ts.UnmarshalJSON(data)
		if err != nil {
			return // invalid input is fine, just must not panic
		}

		// If parsing succeeded, verify the round-trip.
		out, err := ts.MarshalJSON()
		if err != nil {
			t.Fatalf("MarshalJSON failed after successful UnmarshalJSON: %v", err)
		}

		var ts2 Timestamp

		err = ts2.UnmarshalJSON(out)
		if err != nil {
			t.Fatalf("round-trip UnmarshalJSON failed on %q: %v", out, err)
		}

		// Microsecond precision: the round-trip must preserve the stored value.
		if ts.GetData() != ts2.GetData() {
			t.Fatalf("round-trip mismatch: %d vs %d (input=%q, output=%q)",
				ts.GetData(), ts2.GetData(), data, out)
		}
	})
}
