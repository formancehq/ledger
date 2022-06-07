package sqlstorage

import "time"

type TxsPaginationToken struct {
	AfterTxID         uint64    `json:"after"`
	ReferenceFilter   string    `json:"reference,omitempty"`
	AccountFilter     string    `json:"account,omitempty"`
	SourceFilter      string    `json:"source,omitempty"`
	DestinationFilter string    `json:"destination,omitempty"`
	StartTime         time.Time `json:"start_time,omitempty"`
	EndTime           time.Time `json:"end_time,omitempty"`
}

type AccPaginationToken struct {
	Limit               uint              `json:"limit"`
	Offset              uint              `json:"offset"`
	AfterAddress        string            `json:"after,omitempty"`
	AddressRegexpFilter string            `json:"address,omitempty"`
	MetadataFilter      map[string]string `json:"metadata,omitempty"`
}
