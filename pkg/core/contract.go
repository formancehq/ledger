package core

import (
	"encoding/json"
	"regexp"
	"strings"
)

type Contract struct {
	Name    string `json:"name"`
	Account string `json:"account"`
	Expr    Expr   `json:"expr"`
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
		Expr:    expr,
		Account: aux.Account,
		Name:    aux.Name,
	}
	return nil
}

func (c Contract) Match(addr string) bool {
	r := strings.ReplaceAll(c.Account, "*", ".*")
	return regexp.MustCompile(r).Match([]byte(addr))
}
