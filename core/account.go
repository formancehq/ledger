package core

import "strings"

const (
	WORLD                     = "world"
	ADDRESS_SEGMENT_DELIMITER = ":"
)

type Account struct {
	Address  string                      `json:"address" example:"users:001"`
	Contract string                      `json:"contract" example:"default"`
	Type     string                      `json:"type,omitempty" example:"virtual"`
	Balances map[string]int64            `json:"balances,omitempty" example:"COIN:100"`
	Volumes  map[string]map[string]int64 `json:"volumes,omitempty"`
	Metadata Metadata                    `json:"metadata" swaggertype:"object"`
}

func (a Account) Segments() []string {
	return strings.Split(a.Address, ADDRESS_SEGMENT_DELIMITER)
}

func (a Account) CanEmit() bool {
	if a.Address == WORLD {
		return true
	}

	if segments := a.Segments(); len(segments) > 0 && segments[0] == WORLD {
		return true
	}

	return false
}
