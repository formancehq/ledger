package query

var DefaultComparisonOperatorsMapping = map[string]string{
	"$match": "=",
	"$gte":   ">=",
	"$gt":    ">",
	"$lte":   "<=",
	"$lt":    "<",
}
