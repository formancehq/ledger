package commonpb

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/invopop/jsonschema"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// Value implements driver.Valuer for Volumes (for database storage).
func (v *Volumes) Value() (driver.Value, error) {
	if v == nil {
		return nil, nil
	}

	return fmt.Sprintf("(%s, %s)", v.GetInput(), v.GetOutput()), nil
}

// Scan implements sql.Scanner for Volumes (for database reading).
func (v *Volumes) Scan(src any) error {
	if src == nil {
		return nil
	}
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("Volumes.Scan: expected string, got %T", src)
	}
	// stored as (input, output)
	parts := strings.Split(s[1:(len(s)-1)], ",")

	v.Input = strings.TrimSpace(parts[0])
	v.Output = strings.TrimSpace(parts[1])

	return nil
}

// JSONSchemaExtend extends the JSON schema for Volumes.
func (*Volumes) JSONSchemaExtend(schema *jsonschema.Schema) {
	inputProperty, _ := schema.Properties.Get("input")
	schema.Properties.Set("balance", inputProperty)
}

// NewEmptyVolumes creates new empty volumes.
func NewEmptyVolumes() *Volumes {
	return NewVolumesInt64(0, 0)
}

// NewVolumesInt64 creates new volumes from int64 values.
func NewVolumesInt64(input, output int64) *Volumes {
	return &Volumes{
		Input:  big.NewInt(input).String(),
		Output: big.NewInt(output).String(),
	}
}

// Balance calculates the balance (input - output).
func (v *Volumes) Balance() *big.Int {
	if v == nil {
		return big.NewInt(0)
	}

	input, _ := new(big.Int).SetString(v.GetInput(), 10)
	output, _ := new(big.Int).SetString(v.GetOutput(), 10)

	if input == nil {
		input = big.NewInt(0)
	}

	if output == nil {
		output = big.NewInt(0)
	}

	return new(big.Int).Sub(input, output)
}

// MarshalJSON implements json.Marshaler for Volumes.
func (v *Volumes) MarshalJSON() ([]byte, error) {
	if v == nil {
		return json.Marshal(nil)
	}

	balance := v.Balance()
	input, _ := new(big.Int).SetString(v.GetInput(), 10)
	output, _ := new(big.Int).SetString(v.GetOutput(), 10)

	if input == nil {
		input = big.NewInt(0)
	}

	if output == nil {
		output = big.NewInt(0)
	}

	return json.Marshal(VolumesWithBalance{
		Input:   input.String(),
		Output:  output.String(),
		Balance: balance.String(),
	})
}

// SortVolumes orders the inner volumes list by (asset, color) ascending.
// Stable order is required so JSON / proto output is deterministic across
// reads and so snapshot tests don't flap.
func (v *VolumesByAssets) SortVolumes() {
	if v == nil {
		return
	}
	sort.Slice(v.GetVolumes(), func(i, j int) bool {
		if a, b := v.GetVolumes()[i].GetAsset(), v.GetVolumes()[j].GetAsset(); a != b {
			return a < b
		}

		return v.GetVolumes()[i].GetColor() < v.GetVolumes()[j].GetColor()
	})
}

// FindVolume returns the *Volumes for a given (asset, color) tuple, or nil
// when no entry matches. Color "" is the uncolored bucket.
//
// VolumesByAssets is a sorted list, so this is an O(n) linear scan. For
// repeated lookups, callers should build their own map.
func (v *VolumesByAssets) FindVolume(asset, color string) *Volumes {
	if entry := v.findVolumeEntry(asset, color); entry != nil {
		return entry.GetVolumes()
	}

	return nil
}

// findVolumeEntry returns the *VolumeEntry matching (asset, color), or nil.
func (v *VolumesByAssets) findVolumeEntry(asset, color string) *VolumeEntry {
	if v == nil {
		return nil
	}
	for _, entry := range v.GetVolumes() {
		if entry.GetAsset() == asset && entry.GetColor() == color {
			return entry
		}
	}

	return nil
}

// SortVolumes sorts every per-account volume list deterministically.
func (a *PostCommitVolumes) SortVolumes() {
	if a == nil {
		return
	}
	for _, vba := range a.GetVolumesByAccount() {
		vba.SortVolumes()
	}
}
