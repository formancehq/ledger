package system

type ListLedgersQueryPayload struct {
	Bucket         string
	Features       map[string]string
	IncludeDeleted bool
}
