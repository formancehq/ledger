package commonpb

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBigIntegerRoundTrips(t *testing.T) {
	t.Parallel()

	values := []string{
		"0",
		"9007199254740993",     // > 2^53
		"18446744073709551616", // > uint64
		"115792089237316195423570985008687907853269984665640564039457584007913129639935", // 2^256-1
		"115792089237316195423570985008687907853269984665640564039457584007913129639936", // 2^256
	}

	for _, decimal := range values {
		t.Run(decimal, func(t *testing.T) {
			t.Parallel()
			unsigned := MustBigUintFromDecimal(decimal)
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsigned)
			require.NoError(t, err)
			var decoded BigUint
			require.NoError(t, proto.Unmarshal(wire, &decoded))
			require.Equal(t, decimal, decoded.DecimalString())

			encodedJSON, err := json.Marshal(unsigned)
			require.NoError(t, err)
			require.Equal(t, `"`+decimal+`"`, string(encodedJSON))
			var decodedJSON BigUint
			require.NoError(t, json.Unmarshal(encodedJSON, &decodedJSON))
			require.True(t, proto.Equal(unsigned, &decodedJSON))
		})
	}
}

func TestBigIntSignedBalance(t *testing.T) {
	t.Parallel()
	for _, decimal := range []string{"0", "-9007199254740993", "18446744073709551616"} {
		value := MustSignedBigIntFromDecimal(decimal)
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		require.Equal(t, `"`+decimal+`"`, string(encoded))
		var decoded SignedBigInt
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Equal(t, decimal, decoded.DecimalString())
	}
}

func TestBigIntegerRejectsNonCanonicalEncodings(t *testing.T) {
	t.Parallel()

	for _, input := range []string{`0`, `""`, `"00"`, `"01"`, `"+1"`, `"-1"`, `"1.0"`, `"1e3"`} {
		var value BigUint
		require.Error(t, json.Unmarshal([]byte(input), &value), input)
	}
	for _, input := range []string{`0`, `""`, `"-"`, `"-0"`, `"00"`, `"-01"`, `"+1"`, `"1.0"`, `"1e3"`} {
		var value SignedBigInt
		require.Error(t, json.Unmarshal([]byte(input), &value), input)
	}

	require.Error(t, (&BigUint{Magnitude: []byte{0, 1}}).Validate())
	require.Error(t, (&SignedBigInt{Negative: true}).Validate())
	require.Error(t, (&SignedBigInt{Negative: true, Magnitude: &BigUint{Magnitude: []byte{0, 1}}}).Validate())
}

func TestNewBigUintRejectsNegative(t *testing.T) {
	t.Parallel()
	_, err := NewBigUint(big.NewInt(-1))
	require.Error(t, err)
}

func TestVolumesDeterministicProtoAndExactJSON(t *testing.T) {
	t.Parallel()

	input := "115792089237316195423570985008687907853269984665640564039457584007913129639936"
	volumes := &VolumesWithBalance{
		Input:   MustBigUintFromDecimal(input),
		Output:  MustBigUintFromDecimal("1"),
		Balance: MustSignedBigIntFromDecimal("115792089237316195423570985008687907853269984665640564039457584007913129639935"),
	}
	require.NoError(t, volumes.Validate())

	first, err := proto.MarshalOptions{Deterministic: true}.Marshal(volumes)
	require.NoError(t, err)
	second, err := proto.MarshalOptions{Deterministic: true}.Marshal(volumes)
	require.NoError(t, err)
	require.Equal(t, first, second)

	encoded, err := json.Marshal(volumes)
	require.NoError(t, err)
	require.JSONEq(t, `{"input":"`+input+`","output":"1","balance":"115792089237316195423570985008687907853269984665640564039457584007913129639935"}`, string(encoded))
}

func TestVolumesRejectInvalidOrInconsistentValues(t *testing.T) {
	t.Parallel()

	require.Error(t, (&Volumes{Input: &BigUint{}, Output: &BigUint{Magnitude: []byte{0, 1}}}).Validate())
	require.Error(t, (&VolumesWithBalance{
		Input:   MustBigUintFromDecimal("2"),
		Output:  MustBigUintFromDecimal("1"),
		Balance: MustSignedBigIntFromDecimal("2"),
	}).Validate())
	_, err := json.Marshal(&VolumesWithBalance{
		Input:   MustBigUintFromDecimal("0"),
		Output:  MustBigUintFromDecimal("0"),
		Balance: &SignedBigInt{Negative: true},
	})
	require.Error(t, err)
}
