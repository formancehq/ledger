package core

const (
	WORLD = "world"
)

type Balances map[string]int64
type Volumes map[string]map[string]int64

func (v Volumes) Balances() Balances {
	balances := Balances{}
	for asset, vv := range v {
		balances[asset] = vv["input"] - vv["output"]
	}
	return balances
}

type AggregatedVolumes map[string]Volumes

type Account struct {
	Address  string           `json:"address" example:"users:001"`
	Type     string           `json:"type,omitempty" example:"virtual"`
	Balances map[string]int64 `json:"balances,omitempty" example:"COIN:100"`
	Volumes  Volumes          `json:"volumes,omitempty"`
	Metadata Metadata         `json:"metadata" swaggertype:"object"`
}
