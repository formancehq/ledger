package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLog(t *testing.T) {

	d := time.Unix(1648542028, 0)

	log1 := NewTransactionLogWithDate(nil, Transaction{}, d)
	log2 := NewTransactionLogWithDate(&log1, Transaction{}, d)
	if !assert.Equal(t, "c92c4ef0d943f91d326c9241bd4de5766b778671969c861dda798a79e5616f44", log2.Hash) {
		return
	}
}
