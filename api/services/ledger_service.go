package services

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
)

// LedgerService -
type LedgerService struct {
}

// NewLedgerService -
func NewLedgerService() *LedgerService {
	return &LedgerService{}
}

// CreateLedgerService -
func CreateLedgerService() *LedgerService {
	return NewLedgerService()
}

// GetLedger -
func (s *LedgerService) GetLedger(c *gin.Context) *ledger.Ledger {
	l, _ := c.Get("ledger")
	return l.(*ledger.Ledger)
}

// GetLedgerTransactions -
func (s *LedgerService) GetLedgerTransactions(c *gin.Context) (query.Cursor, error) {
	ledger := s.GetLedger(c)
	return ledger.FindTransactions(
		query.After(c.Query("after")),
		query.Reference(c.Query("reference")),
	)
}
