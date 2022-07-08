package sqlstorage

import (
	"time"

	"github.com/numary/ledger/pkg/storage"
)

type TxsPaginationToken struct {
	AfterTxID         uint64            `json:"after"`
	ReferenceFilter   string            `json:"reference,omitempty"`
	AccountFilter     string            `json:"account,omitempty"`
	SourceFilter      string            `json:"source,omitempty"`
	DestinationFilter string            `json:"destination,omitempty"`
	StartTime         time.Time         `json:"start_time,omitempty"`
	EndTime           time.Time         `json:"end_time,omitempty"`
	MetadataFilter    map[string]string `json:"metadata,omitempty"`
	PageSize          uint              `json:"page_size,omitempty"`
}

type AccPaginationToken struct {
	PageSize              uint                    `json:"page_size"`
	Offset                uint                    `json:"offset"`
	AfterAddress          string                  `json:"after,omitempty"`
	AddressRegexpFilter   string                  `json:"address,omitempty"`
	MetadataFilter        map[string]string       `json:"metadata,omitempty"`
	BalanceFilter         string                  `json:"balance,omitempty"`
	BalanceOperatorFilter storage.BalanceOperator `json:"balance_operator,omitempty"`
}

type BalancesPaginationToken struct {
	PageSize            uint   `json:"page_size"`
	Offset              uint   `json:"offset"`
	AfterAddress        string `json:"after,omitempty"`
	AddressRegexpFilter string `json:"address,omitempty"`
}
