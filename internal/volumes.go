package ledger

import (
	"encoding/json"
	"math/big"
)

type Volumes struct {
	Inputs  *big.Int `bun:"inputs" json:"input"`
	Outputs *big.Int `bun:"outputs" json:"output"`
}

func (v Volumes) CopyWithZerosIfNeeded() Volumes {
	return Volumes{
		Inputs:  new(big.Int).Set(v.Inputs),
		Outputs: new(big.Int).Set(v.Outputs),
	}
}

func (v Volumes) WithInput(input *big.Int) Volumes {
	v.Inputs = input
	return v
}

func (v Volumes) WithOutput(output *big.Int) Volumes {
	v.Outputs = output
	return v
}

func NewEmptyVolumes() Volumes {
	return Volumes{
		Inputs:  new(big.Int),
		Outputs: new(big.Int),
	}
}

func NewVolumesInt64(input, output int64) Volumes {
	return Volumes{
		Inputs:  big.NewInt(input),
		Outputs: big.NewInt(output),
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
		Input:   v.Inputs,
		Output:  v.Outputs,
		Balance: v.Balance(),
	})
}

func (v Volumes) Balance() *big.Int {
	return new(big.Int).Sub(v.Inputs, v.Outputs)
}

func (v Volumes) copy() Volumes {
	return Volumes{
		Inputs:  new(big.Int).Set(v.Inputs),
		Outputs: new(big.Int).Set(v.Outputs),
	}
}

type BalancesByAssets map[string]*big.Int

type VolumesByAssets map[string]Volumes

type BalancesByAssetsByAccounts map[string]BalancesByAssets

func (v VolumesByAssets) Balances() BalancesByAssets {
	balances := BalancesByAssets{}
	for asset, vv := range v {
		balances[asset] = new(big.Int).Sub(vv.Inputs, vv.Outputs)
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
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Inputs:  input,
				Outputs: &big.Int{},
			},
		}
	} else {
		volumes := assetsVolumes[asset].CopyWithZerosIfNeeded()
		volumes.Inputs.Add(volumes.Inputs, input)
		assetsVolumes[asset] = volumes
	}
}

func (a PostCommitVolumes) AddOutput(account, asset string, output *big.Int) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Outputs: output,
				Inputs:  &big.Int{},
			},
		}
	} else {
		volumes := assetsVolumes[asset].CopyWithZerosIfNeeded()
		volumes.Outputs.Add(volumes.Outputs, output)
		assetsVolumes[asset] = volumes
	}
}

func (a PostCommitVolumes) Copy() PostCommitVolumes {
	ret := PostCommitVolumes{}
	for key, volumes := range a {
		ret[key] = volumes.copy()
	}
	return ret
}
