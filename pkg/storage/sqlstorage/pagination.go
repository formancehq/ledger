package sqlstorage

import (
	"time"

	"github.com/formancehq/ledger/pkg/ledger"
)

type TxsPaginationToken struct {
	AfterTxID         uint64            `json:"after"`
	ReferenceFilter   string            `json:"reference,omitempty"`
	AccountFilter     string            `json:"account,omitempty"`
	SourceFilter      string            `json:"source,omitempty"`
	DestinationFilter string            `json:"destination,omitempty"`
	StartTime         time.Time         `json:"startTime,omitempty"`
	EndTime           time.Time         `json:"endTime,omitempty"`
	MetadataFilter    map[string]string `json:"metadata,omitempty"`
	PageSize          uint              `json:"pageSize,omitempty"`
}

type AccPaginationToken struct {
	PageSize              uint                   `json:"pageSize"`
	Offset                uint                   `json:"offset"`
	AfterAddress          string                 `json:"after,omitempty"`
	AddressRegexpFilter   string                 `json:"address,omitempty"`
	MetadataFilter        map[string]string      `json:"metadata,omitempty"`
	BalanceFilter         string                 `json:"balance,omitempty"`
	BalanceOperatorFilter ledger.BalanceOperator `json:"balanceOperator,omitempty"`
}

type BalancesPaginationToken struct {
	PageSize            uint   `json:"pageSize"`
	Offset              uint   `json:"offset"`
	AfterAddress        string `json:"after,omitempty"`
	AddressRegexpFilter string `json:"address,omitempty"`
}

type LogsPaginationToken struct {
	AfterID   uint64    `json:"after"`
	PageSize  uint      `json:"pageSize,omitempty"`
	StartTime time.Time `json:"startTime,omitempty"`
	EndTime   time.Time `json:"endTime,omitempty"`
}
