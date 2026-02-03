package commonpb

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/compat/json"
	"github.com/invopop/jsonschema"
)

// Value implements driver.Valuer for Volumes (for database storage)
func (v *Volumes) Value() (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	return fmt.Sprintf("(%s, %s)", v.Input, v.Output), nil
}

// Scan implements sql.Scanner for Volumes (for database reading)
func (v *Volumes) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	// stored as (input, output)
	parts := strings.Split(src.(string)[1:(len(src.(string))-1)], ",")

	v.Input = strings.TrimSpace(parts[0])
	v.Output = strings.TrimSpace(parts[1])

	return nil
}

// JSONSchemaExtend extends the JSON schema for Volumes
func (*Volumes) JSONSchemaExtend(schema *jsonschema.Schema) {
	inputProperty, _ := schema.Properties.Get("input")
	schema.Properties.Set("balance", inputProperty)
}

// Copy creates a deep copy of Volumes
func (v *Volumes) Copy() *Volumes {
	if v == nil {
		return &Volumes{}
	}
	return &Volumes{
		Input:  v.Input,
		Output: v.Output,
	}
}

// NewEmptyVolumes creates new empty volumes
func NewEmptyVolumes() *Volumes {
	return NewVolumesInt64(0, 0)
}

// NewVolumesInt64 creates new volumes from int64 values
func NewVolumesInt64(input, output int64) *Volumes {
	return &Volumes{
		Input:  big.NewInt(input).String(),
		Output: big.NewInt(output).String(),
	}
}

// Balance calculates the balance (input - output)
func (v *Volumes) Balance() *big.Int {
	if v == nil {
		return big.NewInt(0)
	}
	input, _ := new(big.Int).SetString(v.Input, 10)
	output, _ := new(big.Int).SetString(v.Output, 10)
	if input == nil {
		input = big.NewInt(0)
	}
	if output == nil {
		output = big.NewInt(0)
	}
	return new(big.Int).Sub(input, output)
}

// MarshalJSON implements json.Marshaler for Volumes
func (v *Volumes) MarshalJSON() ([]byte, error) {
	if v == nil {
		return json.Marshal(nil)
	}
	balance := v.Balance()
	input, _ := new(big.Int).SetString(v.Input, 10)
	output, _ := new(big.Int).SetString(v.Output, 10)
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

// BalancesByAssets is a type alias for map[string]*big.Int
type BalancesByAssets = map[string]*big.Int

// BalancesByAssetsByAccounts is a type alias for map[string]BalancesByAssets
type BalancesByAssetsByAccounts = map[string]BalancesByAssets

// Balances calculates balances from VolumesByAssets
func (v *VolumesByAssets) Balances() BalancesByAssets {
	if v == nil || v.Volumes == nil {
		return BalancesByAssets{}
	}
	balances := BalancesByAssets{}
	for asset, vol := range v.Volumes {
		if vol != nil {
			input, _ := new(big.Int).SetString(vol.Input, 10)
			output, _ := new(big.Int).SetString(vol.Output, 10)
			if input == nil {
				input = big.NewInt(0)
			}
			if output == nil {
				output = big.NewInt(0)
			}
			balances[asset] = new(big.Int).Sub(input, output)
		}
	}
	return balances
}

// Copy creates a deep copy of VolumesByAssets
func (v *VolumesByAssets) Copy() *VolumesByAssets {
	if v == nil {
		return &VolumesByAssets{Volumes: make(map[string]*Volumes)}
	}
	ret := &VolumesByAssets{
		Volumes: make(map[string]*Volumes),
	}
	for key, volumes := range v.Volumes {
		ret.Volumes[key] = volumes.Copy()
	}
	return ret
}

// AddInput adds an input volume to the specified account and asset
func (a *PostCommitVolumes) AddInput(account, asset string, input *big.Int) {
	if a == nil {
		return
	}
	if a.VolumesByAccount == nil {
		a.VolumesByAccount = make(map[string]*VolumesByAssets)
	}
	if _, ok := a.VolumesByAccount[account]; !ok {
		a.VolumesByAccount[account] = &VolumesByAssets{
			Volumes: make(map[string]*Volumes),
		}
	}
	if _, ok := a.VolumesByAccount[account].Volumes[asset]; !ok {
		a.VolumesByAccount[account].Volumes[asset] = NewEmptyVolumes()
	}
	currentInput, _ := new(big.Int).SetString(a.VolumesByAccount[account].Volumes[asset].Input, 10)
	if currentInput == nil {
		currentInput = big.NewInt(0)
	}
	a.VolumesByAccount[account].Volumes[asset].Input = currentInput.Add(currentInput, input).String()
}

// AddOutput adds an output volume to the specified account and asset
func (a *PostCommitVolumes) AddOutput(account, asset string, output *big.Int) {
	if a == nil {
		return
	}
	if a.VolumesByAccount == nil {
		a.VolumesByAccount = make(map[string]*VolumesByAssets)
	}
	if _, ok := a.VolumesByAccount[account]; !ok {
		a.VolumesByAccount[account] = &VolumesByAssets{
			Volumes: make(map[string]*Volumes),
		}
	}
	if _, ok := a.VolumesByAccount[account].Volumes[asset]; !ok {
		a.VolumesByAccount[account].Volumes[asset] = NewEmptyVolumes()
	}
	currentOutput, _ := new(big.Int).SetString(a.VolumesByAccount[account].Volumes[asset].Output, 10)
	if currentOutput == nil {
		currentOutput = big.NewInt(0)
	}
	a.VolumesByAccount[account].Volumes[asset].Output = currentOutput.Add(currentOutput, output).String()
}

// Copy creates a deep copy of PostCommitVolumes
func (a *PostCommitVolumes) Copy() *PostCommitVolumes {
	if a == nil || len(a.VolumesByAccount) == 0 {
		return &PostCommitVolumes{VolumesByAccount: make(map[string]*VolumesByAssets)}
	}
	ret := &PostCommitVolumes{
		VolumesByAccount: make(map[string]*VolumesByAssets),
	}
	for key, volumes := range a.VolumesByAccount {
		ret.VolumesByAccount[key] = volumes.Copy()
	}
	return ret
}

// SubtractPostings subtracts postings from PostCommitVolumes
func (a *PostCommitVolumes) SubtractPostings(postings []*Posting) *PostCommitVolumes {
	if a == nil || len(a.VolumesByAccount) == 0 {
		return &PostCommitVolumes{VolumesByAccount: make(map[string]*VolumesByAssets)}
	}
	ret := a.Copy()
	for _, posting := range postings {
		if posting == nil {
			continue
		}
		ret.AddOutput(posting.Source, posting.Asset, big.NewInt(0).Neg(posting.Amount.Value()))
		ret.AddInput(posting.Destination, posting.Asset, big.NewInt(0).Neg(posting.Amount.Value()))
	}
	return ret
}

// Merge merges volumes into PostCommitVolumes
func (a *PostCommitVolumes) Merge(volumes *PostCommitVolumes) *PostCommitVolumes {
	if a == nil {
		a = &PostCommitVolumes{VolumesByAccount: make(map[string]*VolumesByAssets)}
	}
	if volumes == nil || volumes.VolumesByAccount == nil {
		return a
	}
	for account, volumesByAssets := range volumes.VolumesByAccount {
		if _, ok := a.VolumesByAccount[account]; !ok {
			a.VolumesByAccount[account] = &VolumesByAssets{
				Volumes: make(map[string]*Volumes),
			}
		}
		for asset, vol := range volumesByAssets.Volumes {
			if _, ok := a.VolumesByAccount[account].Volumes[asset]; !ok {
				a.VolumesByAccount[account].Volumes[asset] = NewEmptyVolumes()
			}
			currentInput, _ := new(big.Int).SetString(a.VolumesByAccount[account].Volumes[asset].Input, 10)
			currentOutput, _ := new(big.Int).SetString(a.VolumesByAccount[account].Volumes[asset].Output, 10)
			volInput, _ := new(big.Int).SetString(vol.Input, 10)
			volOutput, _ := new(big.Int).SetString(vol.Output, 10)
			if currentInput == nil {
				currentInput = big.NewInt(0)
			}
			if currentOutput == nil {
				currentOutput = big.NewInt(0)
			}
			if volInput == nil {
				volInput = big.NewInt(0)
			}
			if volOutput == nil {
				volOutput = big.NewInt(0)
			}
			a.VolumesByAccount[account].Volumes[asset].Input = currentInput.Add(currentInput, volInput).String()
			a.VolumesByAccount[account].Volumes[asset].Output = currentOutput.Add(currentOutput, volOutput).String()
		}
	}
	return a
}

// Balances is a type alias for map[string]map[string]*big.Int
type Balances = map[string]map[string]*big.Int
