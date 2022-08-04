package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {

	d := time.Unix(1648542028, 0).UTC()

	log1 := NewTransactionLogWithDate(nil, Transaction{
		TransactionData: TransactionData{
			Metadata: Metadata{},
		},
	}, d)
	log2 := NewTransactionLogWithDate(&log1, Transaction{
		TransactionData: TransactionData{
			Metadata: Metadata{},
		},
	}, d)
	if !assert.Equal(t, "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e", log2.Hash) {
		return
	}
}
