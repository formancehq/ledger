package core

import (
	"regexp"
	"strings"
)

type Contract struct {
	Name    string `json:"name"`
	Account string `json:"account"`
}

func (c Contract) Match(addr string) bool {
	r := strings.ReplaceAll(c.Account, "*", ".*")
	return regexp.MustCompile(r).Match([]byte(addr))
}
