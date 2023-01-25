package wallet

import (
	"fmt"
)

const (
	SubjectTypeLedgerAccount string = "ACCOUNT"
	SubjectTypeWallet        string = "WALLET"
)

type Subject struct {
	Type       string `json:"type"`
	Identifier string `json:"identifier"`
	Balance    string `json:"balance"`
}

func (s Subject) getAccount(chart *Chart) string {
	switch s.Type {
	case SubjectTypeLedgerAccount:
		return s.Identifier
	case SubjectTypeWallet:
		if s.Balance != "" {
			return chart.GetBalanceAccount(s.Identifier, s.Balance)
		}
		return chart.GetMainBalanceAccount(s.Identifier)
	}
	panic("unknown type")
}

func (s Subject) Validate() error {
	if s.Type != SubjectTypeWallet && s.Type != SubjectTypeLedgerAccount {
		return fmt.Errorf("unknown source type: %s", s.Type)
	}
	return nil
}

type Subjects []Subject

func (subjects Subjects) ResolveAccounts(chart *Chart) []string {
	if len(subjects) == 0 {
		subjects = []Subject{DefaultCreditSource}
	}
	resolvedSources := make([]string, 0)
	for _, source := range subjects {
		resolvedSources = append(resolvedSources, source.getAccount(chart))
	}
	return resolvedSources
}

func (subjects Subjects) Validate() error {
	for _, source := range subjects {
		if err := source.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func NewWalletSubject(walletID, balance string) Subject {
	return Subject{
		Type:       SubjectTypeWallet,
		Identifier: walletID,
		Balance:    balance,
	}
}

func NewLedgerAccountSubject(account string) Subject {
	return Subject{
		Type:       SubjectTypeLedgerAccount,
		Identifier: account,
	}
}
