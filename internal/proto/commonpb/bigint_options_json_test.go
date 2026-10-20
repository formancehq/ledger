package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

func TestBigIntegerEncodingOutsidePosting(t *testing.T) {
	t.Parallel()
	value := NewUint256FromUint64(9007199254740993)
	for name, payload := range map[string]any{
		"scalar": value,
		"nested": map[string]any{"values": []any{struct {
			Value *Uint256 `json:"value"`
		}{Value: value}}},
		"aggregate":         &AggregatedVolume{Input: value},
		"custom projection": &Transaction{Postings: []*Posting{{Amount: value}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.MarshalWithOptions(payload, BigIntegersAsStrings())
			require.NoError(t, err)
			require.Contains(t, string(encoded), `"9007199254740993"`)
		})
	}
}

func TestBigIntegerEncodingBoundaries(t *testing.T) {
	t.Parallel()
	const maxUint256 = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	const beyondUint256 = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	var maximum Uint256
	require.NoError(t, maximum.UnmarshalJSON([]byte(maxUint256)))
	for _, tc := range []struct {
		name   string
		value  any
		number string
		quoted string
	}{
		{"uint256 zero", &Uint256{}, "0", `"0"`},
		{"uint256 max", &maximum, maxUint256, `"` + maxUint256 + `"`},
		{"unsigned zero", &BigUint{}, "0", `"0"`},
		{"unsigned unbounded", MustBigUintFromDecimal(beyondUint256), beyondUint256, `"` + beyondUint256 + `"`},
		{"signed zero", &SignedBigInt{}, "0", `"0"`},
		{"signed negative", MustSignedBigIntFromDecimal("-" + beyondUint256), "-" + beyondUint256, `"-` + beyondUint256 + `"`},
		{"nil uint256", (*Uint256)(nil), "null", "null"},
		{"nil unsigned", (*BigUint)(nil), "null", "null"},
		{"nil signed", (*SignedBigInt)(nil), "null", "null"},
		{"uint64", uint64(9007199254740993), "9007199254740993", "9007199254740993"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range []struct {
				strings bool
				want    string
			}{{false, tc.number}, {true, tc.quoted}} {
				options := BigIntegersAsNumbers()
				if mode.strings {
					options = BigIntegersAsStrings()
				}
				encoded, err := json.MarshalWithOptions(tc.value, options)
				require.NoError(t, err)
				require.Equal(t, mode.want, string(encoded))
				nested := map[string]any{"values": []any{struct {
					Value any    `json:"value"`
					ID    uint64 `json:"id"`
				}{Value: tc.value, ID: 9007199254740993}}}
				encoded, err = json.MarshalWithOptions(nested, options)
				require.NoError(t, err)
				require.Equal(t, `{"values":[{"value":`+mode.want+`,"id":9007199254740993}]}`, string(encoded))
			}
		})
	}
}
