package core

const (
	WORLD = "world"
)

type Account struct {
	Address  string        `json:"address" example:"users:001"`
	Type     string         `json:"type,omitempty" example:"virtual"`
	Balances AssetsBalances `json:"balances,omitempty" example:"COIN:100"`
	Volumes  AssetsVolumes  `json:"volumes,omitempty"`
	Metadata Metadata      `json:"metadata" swaggertype:"object"`
}
