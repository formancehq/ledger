package ledger

type GetAggregatedVolumesOptions struct {
	UseInsertionDate bool `json:"useInsertionDate"`
}

type GetVolumesOptions struct {
	UseInsertionDate bool `json:"insertionDate"`
	GroupLvl         int  `json:"groupBy"`
}
