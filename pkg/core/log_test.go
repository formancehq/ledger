package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {

	d := time.Unix(1648542028, 0).UTC()

	log1 := NewTransactionLogWithDate(nil, Transaction{}, d)
	log2 := NewTransactionLogWithDate(&log1, Transaction{}, d)
	if !assert.Equal(t, "3070ef3437354b5cb5ece914f8610d8d1276c6a9df127c0d2a49c48e3f81b017", log2.Hash) {
		return
	}
}
