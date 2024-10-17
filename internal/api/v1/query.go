package v1

import (
	"net/http"
)

const (
	MaxPageSize     = 1000
	DefaultPageSize = 15

	QueryKeyCursor          = "cursor"
	QueryKeyBalanceOperator = "balanceOperator"
)

func getBalanceOperator(c *http.Request) string {
	balanceOperator := "eq"
	balanceOperatorStr := c.URL.Query().Get(QueryKeyBalanceOperator)
	if balanceOperatorStr != "" {
		return balanceOperatorStr
	}

	return balanceOperator
}
