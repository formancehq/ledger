package wallet

import (
	"net/http"

	"github.com/formancehq/go-libs/metadata"
)

var DefaultCreditSource = NewLedgerAccountSubject("world")

type CreditRequest struct {
	Amount    Monetary          `json:"amount"`
	Metadata  metadata.Metadata `json:"metadata"`
	Sources   Subjects          `json:"sources"`
	Reference string            `json:"reference"`
	Balance   string            `json:"balance"`
}

func (c *CreditRequest) Bind(r *http.Request) error {
	return nil
}

func (c CreditRequest) Validate() error {
	if err := c.Sources.Validate(); err != nil {
		return err
	}
	return nil
}

type Credit struct {
	CreditRequest
	WalletID string `json:"walletID"`
}

func (c Credit) destinationAccount(chart *Chart) string {
	if c.Balance == "" {
		return chart.GetMainBalanceAccount(c.WalletID)
	}
	return chart.GetBalanceAccount(c.WalletID, c.Balance)
}
