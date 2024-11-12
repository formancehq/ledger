package cmd

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/random"
)

func randomBigInt() *big.Int {
	v := random.GetRandom()
	ret := big.NewInt(0)
	ret.SetString(fmt.Sprintf("%d", v), 10)
	return ret
}

type IDSeq struct {
	sync.Mutex
	Count int64
	Sum   *big.Int
}

func NewIDSeq() *IDSeq {
	return &IDSeq{
		Count: 0,
		Sum:   big.NewInt(0),
	}
}

func (s *IDSeq) Register(id *big.Int) {
	s.Lock()
	defer s.Unlock()

	s.Count++
	s.Sum.Add(s.Sum, id)
}

func (s *IDSeq) Check() error {
	s.Lock()
	defer s.Unlock()

	// As the IDs are generated sequentially, the
	// expected sum is the sum of the first n integers
	// where n is the number of IDs generated.
	expectedSum := big.NewInt(0).Div(
		big.NewInt(0).Mul(
			big.NewInt(s.Count-1),
			big.NewInt(0).Add(big.NewInt(s.Count-1), big.NewInt(1)),
		),
		big.NewInt(2),
	)

	if s.Sum.Cmp(expectedSum) != 0 {
		return fmt.Errorf("sum of IDs is incorrect")
	}

	return nil
}
