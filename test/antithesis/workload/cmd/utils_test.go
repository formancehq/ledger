package cmd

import (
	"fmt"
	"math/big"
	"testing"
)

func TestIDSeq(t *testing.T) {
	idSeq := NewIDSeq()

	for i := 0; i < 10; i++ {
		idSeq.Register(big.NewInt(int64(i)))
	}

	if err := idSeq.Check(); err != nil {
		fmt.Println(idSeq.Count, idSeq.Sum)
		t.Errorf("IDSeq check failed")
	}
}
