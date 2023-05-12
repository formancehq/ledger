package core

import (
	"database/sql/driver"
	"encoding/json"
	"math/big"
)

type Volumes struct {
	Input  *big.Int `json:"input"`
	Output *big.Int `json:"output"`
}

func (v Volumes) CopyWithZerosIfNeeded() Volumes {
	var input *big.Int
	if v.Input == nil {
		input = big.NewInt(0)
	} else {
		input = new(big.Int).Set(v.Input)
	}
	var output *big.Int
	if v.Output == nil {
		output = big.NewInt(0)
	} else {
		output = new(big.Int).Set(v.Output)
	}
	return Volumes{
		Input:  input,
		Output: output,
	}
}

func (v Volumes) WithInput(input *big.Int) Volumes {
	v.Input = input
	return v
}

func (v Volumes) WithOutput(output *big.Int) Volumes {
	v.Output = output
	return v
}

func NewEmptyVolumes() Volumes {
	return Volumes{
		Input:  big.NewInt(0),
		Output: big.NewInt(0),
	}
}

type VolumesWithBalance struct {
	Input   *big.Int `json:"input"`
	Output  *big.Int `json:"output"`
	Balance *big.Int `json:"balance"`
}

func (v Volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(VolumesWithBalance{
		Input:   v.Input,
		Output:  v.Output,
		Balance: v.Balance(),
	})
}

func (v Volumes) Balance() *big.Int {
	input := v.Input
	if input == nil {
		input = Zero
	}
	output := v.Output
	if output == nil {
		output = Zero
	}
	return big.NewInt(0).Sub(input, output)
}

func (v Volumes) copy() Volumes {
	return Volumes{
		Input:  new(big.Int).Set(v.Input),
		Output: new(big.Int).Set(v.Output),
	}
}

type AssetsBalances map[string]*big.Int

type AssetsVolumes map[string]Volumes

type AccountsBalances map[string]AssetsBalances

func (v AssetsVolumes) Balances() AssetsBalances {
	balances := AssetsBalances{}
	for asset, vv := range v {
		balances[asset] = new(big.Int).Sub(vv.Input, vv.Output)
	}
	return balances
}

func (v AssetsVolumes) copy() AssetsVolumes {
	ret := AssetsVolumes{}
	for key, volumes := range v {
		ret[key] = volumes.copy()
	}
	return ret
}

type AccountsAssetsVolumes map[string]AssetsVolumes

func (a AccountsAssetsVolumes) GetVolumes(account, asset string) Volumes {
	if a == nil {
		return Volumes{
			Input:  big.NewInt(0),
			Output: big.NewInt(0),
		}
	}
	if assetsVolumes, ok := a[account]; !ok {
		return Volumes{
			Input:  big.NewInt(0),
			Output: big.NewInt(0),
		}
	} else {
		return Volumes{
			Input:  assetsVolumes[asset].Input,
			Output: assetsVolumes[asset].Output,
		}
	}
}

func (a *AccountsAssetsVolumes) SetVolumes(account, asset string, volumes Volumes) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: volumes.CopyWithZerosIfNeeded(),
		}
	} else {
		assetsVolumes[asset] = volumes.CopyWithZerosIfNeeded()
	}
}

func (a *AccountsAssetsVolumes) AddInput(account, asset string, input *big.Int) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: {
				Input:  input,
				Output: big.NewInt(0),
			},
		}
	} else {
		volumes := assetsVolumes[asset].CopyWithZerosIfNeeded()
		volumes.Input.Add(volumes.Input, input)
		assetsVolumes[asset] = volumes
	}
}

func (a *AccountsAssetsVolumes) AddOutput(account, asset string, output *big.Int) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: {
				Output: output,
				Input:  big.NewInt(0),
			},
		}
	} else {
		volumes := assetsVolumes[asset].CopyWithZerosIfNeeded()
		volumes.Output.Add(volumes.Output, output)
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) HasAccount(account string) bool {
	if a == nil {
		return false
	}
	_, ok := a[account]
	return ok
}

func (a AccountsAssetsVolumes) HasAccountAndAsset(account, asset string) bool {
	if a == nil {
		return false
	}
	volumesByAsset, ok := a[account]
	if !ok {
		return false
	}
	_, ok = volumesByAsset[asset]
	return ok
}

// Scan - Implement the database/sql scanner interface
func (a *AccountsAssetsVolumes) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	val, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	*a = AccountsAssetsVolumes{}
	switch val := val.(type) {
	case []uint8:
		return json.Unmarshal(val, a)
	case string:
		return json.Unmarshal([]byte(val), a)
	default:
		panic("not handled type")
	}
}

func (a AccountsAssetsVolumes) copy() AccountsAssetsVolumes {
	ret := AccountsAssetsVolumes{}
	for key, volumes := range a {
		ret[key] = volumes.copy()
	}
	return ret
}

func AggregatePreCommitVolumes(txs ...ExpandedTransaction) AccountsAssetsVolumes {
	ret := AccountsAssetsVolumes{}
	for i := 0; i < len(txs); i++ {
		tx := txs[i]
		for _, posting := range tx.Postings {
			if !ret.HasAccountAndAsset(posting.Source, posting.Asset) {
				ret.SetVolumes(posting.Source, posting.Asset,
					tx.PreCommitVolumes.GetVolumes(posting.Source, posting.Asset))
			}
			if !ret.HasAccountAndAsset(posting.Destination, posting.Asset) {
				ret.SetVolumes(posting.Destination, posting.Asset,
					tx.PreCommitVolumes.GetVolumes(posting.Destination, posting.Asset))
			}
		}
	}
	return ret
}
