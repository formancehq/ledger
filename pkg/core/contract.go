package core

import (
	"encoding/json"
	"regexp"
	"strings"
)

type Contract struct {
	ID      string `json:"id"`
	Expr    Expr   `json:"expr"`
	Account string `json:"account"`
}

func (c *Contract) UnmarshalJSON(data []byte) error {
	type AuxContract Contract
	type Aux struct {
		AuxContract
		Expr map[string]interface{} `json:"expr"`
	}
	aux := Aux{}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	expr, err := ParseRuleExpr(aux.Expr)
	if err != nil {
		return err
	}
	*c = Contract{
		ID:      aux.ID,
		Expr:    expr,
		Account: aux.Account,
	}
	return nil
}

func (c Contract) Match(addr string) bool {
	r := strings.ReplaceAll(c.Account, "*", ".*")
	return regexp.MustCompile(r).Match([]byte(addr))
}
