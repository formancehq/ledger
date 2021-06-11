package core

type Account struct {
	Address  string           `json:"address"`
	Contract string           `json:"contract"`
	Type     string           `json:"type,omitempty"`
	Balances map[string]int64 `json:"balances,omitempty"`
}
