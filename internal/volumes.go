package ledger

import (
	"encoding/json"
	"github.com/invopop/jsonschema"
	"math/big"
)

type Volumes struct {
	Input  *big.Int `json:"input"`
	Output *big.Int `json:"output"`
}

func (Volumes) JSONSchemaExtend(schema *jsonschema.Schema) {
	inputProperty, _ := schema.Properties.Get("input")
	schema.Properties.Set("balance", inputProperty)
}

func (v Volumes) Copy() Volumes {
	return Volumes{
		Input:  new(big.Int).Set(v.Input),
		Output: new(big.Int).Set(v.Output),
	}
}

func NewEmptyVolumes() Volumes {
	return NewVolumesInt64(0, 0)
}

func NewVolumesInt64(input, output int64) Volumes {
	return Volumes{
		Input:  big.NewInt(input),
		Output: big.NewInt(output),
	}
}

type VolumesWithBalanceByAssetByAccount struct {
	Account string `json:"account" bun:"account"`
	Asset   string `json:"asset" bun:"asset"`
	VolumesWithBalance
}

type VolumesWithBalance struct {
	Input   *big.Int `json:"input" bun:"input"`
	Output  *big.Int `json:"output" bun:"output"`
	Balance *big.Int `json:"balance" bun:"balance"`
}

type VolumesWithBalanceByAssets map[string]*VolumesWithBalance

func (v Volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(VolumesWithBalance{
		Input:   v.Input,
		Output:  v.Output,
		Balance: v.Balance(),
	})
}

func (v Volumes) Balance() *big.Int {
	return new(big.Int).Sub(v.Input, v.Output)
}

func (v Volumes) copy() Volumes {
	return Volumes{
		Input:  new(big.Int).Set(v.Input),
		Output: new(big.Int).Set(v.Output),
	}
}

type BalancesByAssets map[string]*big.Int

type VolumesByAssets map[string]Volumes

type BalancesByAssetsByAccounts map[string]BalancesByAssets

func (v VolumesByAssets) Balances() BalancesByAssets {
	balances := BalancesByAssets{}
	for asset, vv := range v {
		balances[asset] = new(big.Int).Sub(vv.Input, vv.Output)
	}
	return balances
}

func (v VolumesByAssets) copy() VolumesByAssets {
	ret := VolumesByAssets{}
	for key, volumes := range v {
		ret[key] = volumes.copy()
	}
	return ret
}

type PostCommitVolumes map[string]VolumesByAssets

func (a PostCommitVolumes) AddInput(account, asset string, input *big.Int) {
	volumes := a[account][asset].Copy()
	volumes.Input.Add(volumes.Input, input)
	a[account][asset] = volumes
}

func (a PostCommitVolumes) AddOutput(account, asset string, output *big.Int) {
	volumes := a[account][asset].Copy()
	volumes.Output.Add(volumes.Output, output)
	a[account][asset] = volumes
}

func (a PostCommitVolumes) Copy() PostCommitVolumes {
	ret := PostCommitVolumes{}
	for key, volumes := range a {
		ret[key] = volumes.copy()
	}
	return ret
}

func (a PostCommitVolumes) Merge(volumes PostCommitVolumes) PostCommitVolumes {
	for account, volumesByAssets := range volumes {
		if _, ok := a[account]; !ok {
			a[account] = map[string]Volumes{}
		}
		for asset, volumes := range volumesByAssets {
			if _, ok := a[account][asset]; !ok {
				a[account][asset] = NewEmptyVolumes()
			}
			a[account][asset].Input.Add(a[account][asset].Input, volumes.Input)
			a[account][asset].Output.Add(a[account][asset].Output, volumes.Output)
		}
	}

	return a
}
