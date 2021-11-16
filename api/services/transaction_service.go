package services

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger/query"
)

// TransactionService -
type TransactionService struct {
	ledgerService *LedgerService
}

// NewTransactionService -
func NewTransactionService(
	ledgerService *LedgerService,
) *TransactionService {
	return &TransactionService{
		ledgerService: ledgerService,
	}
}

// CreateTransactionService -
func CreateTransactionService() *TransactionService {
	return NewTransactionService(
		CreateLedgerService(),
	)
}

// GetTransactions -
func (s *TransactionService) GetTransactions(c *gin.Context) (query.Cursor, error) {
	return s.ledgerService.GetLedgerTransactions(c)
}
