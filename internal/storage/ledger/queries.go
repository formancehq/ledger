package ledger

type GetAggregatedVolumesOptions struct {
	UseInsertionDate bool `json:"useInsertionDate"`
}

type GetVolumesOptions struct {
	UseInsertionDate bool `json:"useInsertionDate"`
	GroupLvl         int  `json:"groupLvl"`
}

type BalanceQuery = map[string][]string
