package commonpb

import (
	"database/sql/driver"
	"errors"
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
	if err := v.Validate(); err != nil {
		return nil, fmt.Errorf("Volumes.Value: %w", err)
	}
	input, err := v.GetInput().Dec()
	if err != nil {
		return nil, err
	}
	output, err := v.GetOutput().Dec()
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("(%s, %s)", input, output), nil
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
	if len(s) < 5 || s[0] != '(' || s[len(s)-1] != ')' {
		return fmt.Errorf("Volumes.Scan: malformed tuple %q", s)
	}
	// stored as (input, output)
	parts := strings.Split(s[1:(len(s)-1)], ",")
	if len(parts) != 2 {
		return fmt.Errorf("Volumes.Scan: expected two tuple elements, got %d", len(parts))
	}

	input, err := parseCanonicalBigUint(strings.TrimSpace(parts[0]))
	if err != nil {
		return fmt.Errorf("Volumes.Scan input: %w", err)
	}
	output, err := parseCanonicalBigUint(strings.TrimSpace(parts[1]))
	if err != nil {
		return fmt.Errorf("Volumes.Scan output: %w", err)
	}
	v.Input = input
	v.Output = output

	return nil
}

func (v *Volumes) Validate() error {
	if v == nil {
		return nil
	}
	if v.GetInput() == nil || v.GetOutput() == nil {
		return errors.New("volume input and output must be present")
	}
	if err := v.GetInput().Validate(); err != nil {
		return fmt.Errorf("invalid input volume: %w", err)
	}
	if err := v.GetOutput().Validate(); err != nil {
		return fmt.Errorf("invalid output volume: %w", err)
	}

	return nil
}

// canonicalUnsignedSchema and canonicalSignedSchema are the shared JSON schema
// constraints for all typed amount fields. Centralising them means a pattern
// change propagates to every volume-bearing schema without drift.
var (
	canonicalUnsignedSchema = &jsonschema.Schema{Type: "string", Pattern: `^(0|[1-9][0-9]*)$`}
	canonicalSignedSchema   = &jsonschema.Schema{Type: "string", Pattern: `^(0|-?[1-9][0-9]*)$`}
)

// JSONSchemaExtend extends the JSON schema for Volumes.
func (*Volumes) JSONSchemaExtend(schema *jsonschema.Schema) {
	schema.Properties.Set("input", canonicalUnsignedSchema)
	schema.Properties.Set("output", canonicalUnsignedSchema)
	schema.Properties.Set("balance", canonicalSignedSchema)
}

func (*VolumesWithBalance) JSONSchemaExtend(schema *jsonschema.Schema) {
	schema.Properties.Set("input", canonicalUnsignedSchema)
	schema.Properties.Set("output", canonicalUnsignedSchema)
	schema.Properties.Set("balance", canonicalSignedSchema)
}

// Balance calculates the balance (input - output).
func (v *Volumes) Balance() (*big.Int, error) {
	if v == nil {
		return big.NewInt(0), nil
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}

	input, err := v.GetInput().ToBigInt()
	if err != nil {
		return nil, fmt.Errorf("invalid input volume: %w", err)
	}
	output, err := v.GetOutput().ToBigInt()
	if err != nil {
		return nil, fmt.Errorf("invalid output volume: %w", err)
	}

	return new(big.Int).Sub(input, output), nil
}

// MarshalJSON implements json.Marshaler for Volumes.
func (v *Volumes) MarshalJSON() ([]byte, error) {
	if v == nil {
		return json.Marshal(nil)
	}

	balance, err := v.Balance()
	if err != nil {
		return nil, err
	}

	vwb := &VolumesWithBalance{
		Input:   v.GetInput(),
		Output:  v.GetOutput(),
		Balance: NewSignedBigInt(balance),
	}
	// Marshal via pointer so VolumesWithBalance.MarshalJSON (pointer receiver)
	// runs, calling Validate() and producing the canonical decimal-string shape.
	return json.Marshal(vwb)
}

func (v *VolumesWithBalance) Validate() error {
	if v == nil {
		return nil
	}
	if v.GetInput() == nil || v.GetOutput() == nil || v.GetBalance() == nil {
		return errors.New("volume input, output, and balance must be present")
	}
	input, err := v.GetInput().ToBigInt()
	if err != nil {
		return fmt.Errorf("invalid input volume: %w", err)
	}
	output, err := v.GetOutput().ToBigInt()
	if err != nil {
		return fmt.Errorf("invalid output volume: %w", err)
	}
	balance, err := v.GetBalance().ToBigInt()
	if err != nil {
		return fmt.Errorf("invalid balance: %w", err)
	}
	if want := new(big.Int).Sub(input, output); balance.Cmp(want) != 0 {
		return fmt.Errorf("balance %s does not equal input minus output %s", balance, want)
	}

	return nil
}

func (v *VolumesWithBalance) MarshalJSON() ([]byte, error) {
	if v == nil {
		return json.Marshal(nil)
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(&struct {
		Input   string `json:"input"`
		Output  string `json:"output"`
		Balance string `json:"balance"`
	}{
		Input:   v.GetInput().DecimalString(),
		Output:  v.GetOutput().DecimalString(),
		Balance: v.GetBalance().DecimalString(),
	})
}

func parseCanonicalBigUint(decimal string) (*BigUint, error) {
	// Delegate to the shared validator so SQL scan and JSON decode
	// cannot diverge when the canonical rules change.
	if err := validateCanonicalDecimalString(decimal, false); err != nil {
		return nil, err
	}
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		return nil, fmt.Errorf("invalid integer %q", decimal)
	}
	encoded, err := NewBigUint(value)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

// AssetColored is implemented by every volume-bearing message keyed by an
// (asset, color) tuple. It lets a single comparator order any of them.
type AssetColored interface {
	GetAsset() string
	GetColor() string
}

// LessByAssetColor reports whether a sorts before b by (asset, color)
// ascending. It is the single comparator shared by every deterministic
// volume ordering (VolumeEntry, AccountVolume, AggregatedVolume) so the sort
// key can never drift between call sites.
func LessByAssetColor[T AssetColored](a, b T) bool {
	if x, y := a.GetAsset(), b.GetAsset(); x != y {
		return x < y
	}

	return a.GetColor() < b.GetColor()
}

// SortVolumes orders the inner volumes list by (asset, color) ascending.
// Stable order is required so JSON / proto output is deterministic across
// reads and so snapshot tests don't flap.
func (v *VolumesByAssets) SortVolumes() {
	if v == nil {
		return
	}
	sort.Slice(v.GetVolumes(), func(i, j int) bool {
		return LessByAssetColor(v.GetVolumes()[i], v.GetVolumes()[j])
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
