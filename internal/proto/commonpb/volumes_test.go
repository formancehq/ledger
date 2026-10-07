package commonpb

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func BenchmarkVolumesMarshalJSON(b *testing.B) {
	volumes := &Volumes{Input: MustBigUintFromDecimal("5"), Output: MustBigUintFromDecimal("8")}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := volumes.MarshalJSON(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVolumesWithBalanceMarshalJSON(b *testing.B) {
	volumes := &VolumesWithBalance{
		Input: MustBigUintFromDecimal("5"), Output: MustBigUintFromDecimal("8"),
		Balance: MustSignedBigIntFromDecimal("-3"),
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := volumes.MarshalJSON(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestVolumesMarshalJSONMatchesExplicitBalance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input, output, balance string
	}{
		{"zero", "0", "0", "0"},
		{"positive", "8", "5", "3"},
		{"negative", "5", "8", "-3"},
		{"beyond uint256", "115792089237316195423570985008687907853269984665640564039457584007913129639936", "1", "115792089237316195423570985008687907853269984665640564039457584007913129639935"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			volumes := &Volumes{Input: MustBigUintFromDecimal(tc.input), Output: MustBigUintFromDecimal(tc.output)}
			withBalance := &VolumesWithBalance{Input: volumes.GetInput(), Output: volumes.GetOutput(), Balance: MustSignedBigIntFromDecimal(tc.balance)}
			want := `{"input":"` + tc.input + `","output":"` + tc.output + `","balance":"` + tc.balance + `"}`
			encoded, err := volumes.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, want, string(encoded))
			encoded, err = withBalance.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, want, string(encoded))
		})
	}
}

func TestVolumesWithBalanceMarshalJSONPreservesValidationErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		volumes *VolumesWithBalance
		want    string
	}{
		{"missing input", &VolumesWithBalance{Output: &BigUint{}, Balance: &SignedBigInt{}}, "volume input, output, and balance must be present"},
		{"missing output", &VolumesWithBalance{Input: &BigUint{}, Balance: &SignedBigInt{}}, "volume input, output, and balance must be present"},
		{"missing balance", &VolumesWithBalance{Input: &BigUint{}, Output: &BigUint{}}, "volume input, output, and balance must be present"},
		{"invalid input", &VolumesWithBalance{Input: &BigUint{Magnitude: []byte{0}}, Output: &BigUint{}, Balance: &SignedBigInt{}}, "invalid input volume: big uint magnitude has a leading zero octet"},
		{"invalid output", &VolumesWithBalance{Input: &BigUint{}, Output: &BigUint{Magnitude: []byte{0}}, Balance: &SignedBigInt{}}, "invalid output volume: big uint magnitude has a leading zero octet"},
		{"invalid balance", &VolumesWithBalance{Input: &BigUint{}, Output: &BigUint{}, Balance: &SignedBigInt{Negative: true}}, "invalid balance: negative zero is not a canonical signed big int"},
		{"inconsistent balance", &VolumesWithBalance{Input: &BigUint{}, Output: &BigUint{}, Balance: MustSignedBigIntFromDecimal("1")}, "balance 1 does not equal input minus output 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.EqualError(t, tc.volumes.Validate(), tc.want)
			_, err := tc.volumes.MarshalJSON()
			require.EqualError(t, err, tc.want)
		})
	}
}

func TestVolumesSQLRoundTrip(t *testing.T) {
	t.Parallel()

	volumes := &Volumes{
		Input:  MustBigUintFromDecimal("18446744073709551616"),
		Output: MustBigUintFromDecimal("7"),
	}
	value, err := volumes.Value()
	require.NoError(t, err)
	require.Equal(t, "(18446744073709551616, 7)", value)

	var decoded Volumes
	require.NoError(t, decoded.Scan(value))
	require.Equal(t, "18446744073709551616", decoded.GetInput().DecimalString())
	require.Equal(t, "7", decoded.GetOutput().DecimalString())
	require.NoError(t, decoded.Scan(nil))

	var nilVolumes *Volumes
	value, err = nilVolumes.Value()
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestVolumesSQLRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	for _, value := range []any{42, "", "(1)", "(1,2,3)", "(-1, 0)", "(0, 01)"} {
		var volumes Volumes
		require.Error(t, volumes.Scan(value), value)
	}

	_, err := (&Volumes{Input: &BigUint{Magnitude: []byte{0, 1}}, Output: MustBigUintFromDecimal("0")}).Value()
	require.Error(t, err)
	_, err = (&Volumes{Input: MustBigUintFromDecimal("0"), Output: &BigUint{Magnitude: []byte{0, 1}}}).Value()
	require.Error(t, err)
}

func TestVolumesBalanceAndJSON(t *testing.T) {
	t.Parallel()

	volumes := &Volumes{Input: MustBigUintFromDecimal("5"), Output: MustBigUintFromDecimal("8")}
	balance, err := volumes.Balance()
	require.NoError(t, err)
	require.Equal(t, "-3", balance.String())

	encoded, err := json.Marshal(volumes)
	require.NoError(t, err)
	require.JSONEq(t, `{"input":"5","output":"8","balance":"-3"}`, string(encoded))

	var nilVolumes *Volumes
	balance, err = nilVolumes.Balance()
	require.NoError(t, err)
	require.Zero(t, balance.Sign())
	encoded, err = json.Marshal(nilVolumes)
	require.NoError(t, err)
	require.Equal(t, "null", string(encoded))

	invalid := &Volumes{Input: &BigUint{Magnitude: []byte{0, 1}}, Output: MustBigUintFromDecimal("0")}
	_, err = invalid.Balance()
	require.Error(t, err)
	_, err = json.Marshal(invalid)
	require.Error(t, err)
}

func TestVolumeCollectionsUseAssetColorIdentity(t *testing.T) {
	t.Parallel()

	volumes := &VolumesByAssets{Volumes: []*VolumeEntry{
		{Asset: "USD", Color: "OPS", Volumes: &Volumes{}},
		{Asset: "EUR", Volumes: &Volumes{}},
		{Asset: "USD", Color: "GRANTS", Volumes: &Volumes{}},
	}}
	volumes.SortVolumes()
	require.Equal(t, []string{"EUR|", "USD|GRANTS", "USD|OPS"}, []string{
		volumes.GetVolumes()[0].GetAsset() + "|" + volumes.GetVolumes()[0].GetColor(),
		volumes.GetVolumes()[1].GetAsset() + "|" + volumes.GetVolumes()[1].GetColor(),
		volumes.GetVolumes()[2].GetAsset() + "|" + volumes.GetVolumes()[2].GetColor(),
	})
	require.Same(t, volumes.GetVolumes()[1].GetVolumes(), volumes.FindVolume("USD", "GRANTS"))
	require.Nil(t, volumes.FindVolume("USD", "missing"))

	var nilVolumes *VolumesByAssets
	nilVolumes.SortVolumes()
	require.Nil(t, nilVolumes.FindVolume("USD", ""))

	postCommit := &PostCommitVolumes{VolumesByAccount: map[string]*VolumesByAssets{"alice": volumes}}
	postCommit.SortVolumes()
	var nilPostCommit *PostCommitVolumes
	nilPostCommit.SortVolumes()
}

func TestNewBigUintFromUint256(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0", NewBigUintFromUint256(nil).DecimalString())
	require.Equal(t, "0", NewBigUintFromUint256(new(uint256.Int)).DecimalString())
	value, overflow := uint256.FromBig(new(big.Int).Lsh(big.NewInt(1), 255))
	require.False(t, overflow)
	require.Equal(t, new(big.Int).Lsh(big.NewInt(1), 255).String(), NewBigUintFromUint256(value).DecimalString())
}
