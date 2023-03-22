package core

import (
	"database/sql/driver"
	"encoding/json"
)

type Volumes struct {
	Input  *MonetaryInt `json:"input"`
	Output *MonetaryInt `json:"output"`
}

func (v Volumes) WithInput(input *MonetaryInt) Volumes {
	v.Input = input
	return v
}

func (v Volumes) WithOutput(output *MonetaryInt) Volumes {
	v.Output = output
	return v
}

func NewEmptyVolumes() Volumes {
	return Volumes{
		Input:  NewMonetaryInt(0),
		Output: NewMonetaryInt(0),
	}
}

type VolumesWithBalance struct {
	Input   *MonetaryInt `json:"input"`
	Output  *MonetaryInt `json:"output"`
	Balance *MonetaryInt `json:"balance"`
}

func (v Volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(VolumesWithBalance{
		Input:   v.Input,
		Output:  v.Output,
		Balance: v.Input.Sub(v.Output),
	})
}

func (v Volumes) Balance() *MonetaryInt {
	return v.Input.Sub(v.Output)
}

func (v Volumes) copy() Volumes {
	return Volumes{
		Input:  v.Input.Sub(NewMonetaryInt(0)),  // copy
		Output: v.Output.Sub(NewMonetaryInt(0)), // copy
	}
}

type AssetsBalances map[string]*MonetaryInt

type AssetsVolumes map[string]Volumes

type AccountsBalances map[string]AssetsBalances

func (v AssetsVolumes) Balances() AssetsBalances {
	balances := AssetsBalances{}
	for asset, vv := range v {
		balances[asset] = vv.Input.Sub(vv.Output)
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

func NewAccountsAssetsVolumes() AccountsAssetsVolumes {
	return AccountsAssetsVolumes{}
}

func (a AccountsAssetsVolumes) GetVolumes(account, asset string) Volumes {
	if a == nil {
		return Volumes{
			Input:  NewMonetaryInt(0),
			Output: NewMonetaryInt(0),
		}
	}
	if assetsVolumes, ok := a[account]; !ok {
		return Volumes{
			Input:  NewMonetaryInt(0),
			Output: NewMonetaryInt(0),
		}
	} else {
		return Volumes{
			Input:  assetsVolumes[asset].Input.OrZero(),
			Output: assetsVolumes[asset].Output.OrZero(),
		}
	}
}

func (a *AccountsAssetsVolumes) SetVolumes(account, asset string, volumes Volumes) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: {
				Input:  volumes.Input.OrZero(),
				Output: volumes.Output.OrZero(),
			},
		}
	} else {
		assetsVolumes[asset] = Volumes{
			Input:  volumes.Input.OrZero(),
			Output: volumes.Output.OrZero(),
		}
	}
}

func (a *AccountsAssetsVolumes) AddInput(account, asset string, input *MonetaryInt) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: {
				Input:  input.OrZero(),
				Output: NewMonetaryInt(0),
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Input = volumes.Input.Add(input)
		assetsVolumes[asset] = volumes
	}
}

func (a *AccountsAssetsVolumes) AddOutput(account, asset string, output *MonetaryInt) {
	if *a == nil {
		*a = AccountsAssetsVolumes{}
	}
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]Volumes{
			asset: {
				Output: output.OrZero(),
				Input:  NewMonetaryInt(0),
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Output = volumes.Output.Add(output)
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
