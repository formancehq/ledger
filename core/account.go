package core

const (
	WORLD = "world"
)

type Account struct {
	Address  string                      `json:"address"`
	Contract string                      `json:"contract"`
	Type     string                      `json:"type,omitempty"`
	Balances map[string]int64            `json:"balances,omitempty"`
	Volumes  map[string]map[string]int64 `json:"volumes,omitempty"`
	Metadata Metadata                    `json:"metadata"`
}
