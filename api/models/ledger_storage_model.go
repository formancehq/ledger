package models

// LedgerStorage -
type LedgerStorage struct {
	Driver  interface{} `json:"driver"`
	Ledgers interface{} `json:"ledgers"`
}
