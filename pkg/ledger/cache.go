package ledger

import (
	"github.com/dgraph-io/ristretto"
	"github.com/pkg/errors"
)

func NewCache(capacityBytes, maxNumKeys int64, metrics bool) *ristretto.Cache {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxNumKeys * 10,
		MaxCost:     capacityBytes,
		BufferItems: 64,
		Metrics:     metrics,
	})
	if err != nil {
		panic(errors.Wrap(err, "creating cache"))
	}
	return cache
}
