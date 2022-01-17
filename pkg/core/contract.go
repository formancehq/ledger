package core

import (
	"regexp"
	"strings"
)

type Contract struct {
	ID      string `json:"id"`
	Expr    Expr   `json:"expr"`
	Account string `json:"account"`
}

func (c Contract) Match(addr string) bool {
	r := strings.ReplaceAll(c.Account, "*", ".*")
	return regexp.MustCompile(r).Match([]byte(addr))
}
