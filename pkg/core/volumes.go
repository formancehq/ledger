package core

import (
	"database/sql/driver"
	"encoding/json"
)

type Volumes struct {
	Input  MonetaryInt `json:"input"`
	Output MonetaryInt `json:"output"`
}

type VolumesWithBalance struct {
	Input   MonetaryInt `json:"input"`
	Output  MonetaryInt `json:"output"`
	Balance MonetaryInt `json:"balance"`
}

func (v Volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(VolumesWithBalance{
		Input:   v.Input,
		Output:  v.Output,
		Balance: v.Input - v.Output,
	})
}

func (v Volumes) Balance() MonetaryInt {
	return v.Input - v.Output
}

type AssetsBalances map[string]MonetaryInt
type AssetsVolumes map[string]Volumes

type AccountsBalances map[string]AssetsBalances

func (v AssetsVolumes) Balances() AssetsBalances {
	balances := AssetsBalances{}
	for asset, vv := range v {
		balances[asset] = vv.Input - vv.Output
	}
	return balances
}

type AccountsAssetsVolumes map[string]AssetsVolumes

func (a AccountsAssetsVolumes) GetVolumes(account, asset string) Volumes {
	if assetsVolumes, ok := a[account]; !ok {
		return Volumes{}
	} else {
		return assetsVolumes[asset]
	}
}

func (a AccountsAssetsVolumes) SetVolumes(account, asset string, volumes Volumes) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: volumes,
		}
	} else {
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) AddInput(account, asset string, input MonetaryInt) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Input: input,
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Input = volumes.Input.Add(input)
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) AddOutput(account, asset string, output MonetaryInt) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Output: output,
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Output = volumes.Output.Add(output)
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) HasAccount(account string) bool {
	_, ok := a[account]
	return ok
}

func (a AccountsAssetsVolumes) HasAccountAndAsset(account, asset string) bool {
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

func AggregatePostCommitVolumes(txs ...ExpandedTransaction) AccountsAssetsVolumes {
	ret := AccountsAssetsVolumes{}
	for i := len(txs) - 1; i >= 0; i-- {
		tx := txs[i]
		for _, posting := range tx.Postings {
			if !ret.HasAccountAndAsset(posting.Source, posting.Asset) {
				ret.SetVolumes(posting.Source, posting.Asset,
					tx.PostCommitVolumes.GetVolumes(posting.Source, posting.Asset))
			}
			if !ret.HasAccountAndAsset(posting.Destination, posting.Asset) {
				ret.SetVolumes(posting.Destination, posting.Asset,
					tx.PostCommitVolumes.GetVolumes(posting.Destination, posting.Asset))
			}
		}
	}
	return ret
}
