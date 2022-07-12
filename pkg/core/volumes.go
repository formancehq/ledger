package core

import (
	"database/sql/driver"
	"encoding/json"
)

type Volumes struct {
	Input  int64 `json:"input"`
	Output int64 `json:"output"`
}

type VolumesWithBalance struct {
	Input   int64 `json:"input"`
	Output  int64 `json:"output"`
	Balance int64 `json:"balance"`
}

func (v Volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(VolumesWithBalance{
		Input:   v.Input,
		Output:  v.Output,
		Balance: v.Input - v.Output,
	})
}

func (v Volumes) Balance() int64 {
	return v.Input - v.Output
}

type AssetsBalances map[string]int64
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

func (a AccountsAssetsVolumes) AddInput(account, asset string, input int64) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Input: input,
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Input += input
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) AddOutput(account, asset string, output int64) {
	if assetsVolumes, ok := a[account]; !ok {
		a[account] = map[string]Volumes{
			asset: {
				Output: output,
			},
		}
	} else {
		volumes := assetsVolumes[asset]
		volumes.Output += output
		assetsVolumes[asset] = volumes
	}
}

func (a AccountsAssetsVolumes) HasAccount(account string) bool {
	_, ok := a[account]
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
