// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func (o *LedgerStorage) GetDriver() string {
	if o == nil {
		return ""
	}
	return o.Driver
}

func (o *LedgerStorage) GetLedgers() []string {
	if o == nil {
		return []string{}
	}
	return o.Ledgers
}
