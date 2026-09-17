package commonpb

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/invopop/jsonschema"
	"github.com/stretchr/testify/require"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

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

func TestVolumesSchemaUsesCanonicalDecimalStrings(t *testing.T) {
	t.Parallel()

	volumesSchema := &jsonschema.Schema{Properties: orderedmap.New[string, *jsonschema.Schema]()}
	(&Volumes{}).JSONSchemaExtend(volumesSchema)
	require.Equal(t, `^(0|[1-9][0-9]*)$`, volumesSchema.Properties.Value("input").Pattern)
	require.Equal(t, `^(0|[1-9][0-9]*)$`, volumesSchema.Properties.Value("output").Pattern)

	balancedSchema := &jsonschema.Schema{Properties: orderedmap.New[string, *jsonschema.Schema]()}
	(&VolumesWithBalance{}).JSONSchemaExtend(balancedSchema)
	require.Equal(t, `^(0|-?[1-9][0-9]*)$`, balancedSchema.Properties.Value("balance").Pattern)
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
		volumes.Volumes[0].GetAsset() + "|" + volumes.Volumes[0].GetColor(),
		volumes.Volumes[1].GetAsset() + "|" + volumes.Volumes[1].GetColor(),
		volumes.Volumes[2].GetAsset() + "|" + volumes.Volumes[2].GetColor(),
	})
	require.Same(t, volumes.Volumes[1].GetVolumes(), volumes.FindVolume("USD", "GRANTS"))
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
