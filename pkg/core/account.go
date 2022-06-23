package core

const (
	WORLD = "world"
)

type Account struct {
	Address  string   `json:"address" example:"users:001"`
	Metadata Metadata `json:"metadata" swaggertype:"object"`
}

type AccountWithVolumes struct {
	Account
	Volumes  AssetsVolumes  `json:"volumes"`
	Balances AssetsBalances `json:"balances" example:"COIN:100"`
}
