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
	if !assert.Equal(t, "69595c585c2068dea91e0a918f49e3e00a2e22da54af4b6a506ff57a4f600485", log2.Hash) {
		return
	}
}
