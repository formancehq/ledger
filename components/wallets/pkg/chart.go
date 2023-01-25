package wallet

import (
	"strings"
)

const MainBalance = "main"

type Address []string

func (addr Address) String() string {
	s := strings.Join(addr, ":")
	s = strings.ReplaceAll(s, "-", "")

	return s
}

type Chart struct {
	Prefix string
}

func NewChart(prefix string) *Chart {
	return &Chart{Prefix: prefix}
}

func (c *Chart) BasePath() Address {
	addr := Address{}

	if c.Prefix != "" {
		addr = append(addr, c.Prefix)
	}

	addr = append(addr, "wallets")

	return addr
}

func (c *Chart) GetMainBalanceAccount(walletID string) string {
	return c.GetBalanceAccount(walletID, MainBalance)
}

func (c *Chart) GetHoldAccount(holdID string) string {
	addr := c.BasePath()
	addr = append(addr, "holds")
	addr = append(addr, holdID)

	return addr.String()
}

func (c *Chart) GetBalanceAccount(walletID, balanceName string) string {
	addr := c.BasePath()
	addr = append(addr, walletID, balanceName)

	return addr.String()
}
