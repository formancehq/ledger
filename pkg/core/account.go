package core

const (
	WORLD = "world"
)

type Volume struct {
	Input  int64 `json:"input"`
	Output int64 `json:"output"`
}

type Balances map[string]int64
type Volumes map[string]Volume

func (v Volumes) Balances() Balances {
	balances := Balances{}
	for asset, vv := range v {
		balances[asset] = vv.Input - vv.Output
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
