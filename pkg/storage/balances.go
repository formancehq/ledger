package storage

import "github.com/numary/ledger/pkg/api/struct_api"

type BalancesQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Params       struct_api.GetBalancesStruct
}
