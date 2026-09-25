package accountlifecycle

import (
	"context"
	"slices"
	"sort"

	"golang.org/x/sync/semaphore"

	"github.com/formancehq/ledger/v3/internal/domain"
)

const stripeCount = 64

// Serializer keeps account lifecycle snapshots ordered with every proposal
// producer that can mutate the same account.
type Serializer struct {
	stripes [stripeCount]*semaphore.Weighted
}

func NewSerializer() *Serializer {
	s := &Serializer{}
	for i := range s.stripes {
		s.stripes[i] = semaphore.NewWeighted(1)
	}

	return s
}

func (s *Serializer) Acquire(ctx context.Context, accounts map[domain.AccountKey]struct{}, all bool) (func(), error) {
	set := make(map[int]struct{})
	if all {
		for i := range s.stripes {
			set[i] = struct{}{}
		}
	} else {
		for account := range accounts {
			set[Index(account)] = struct{}{}
		}
	}
	indexes := make([]int, 0, len(set))
	for index := range set {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	acquired := 0
	for _, index := range indexes {
		if err := s.stripes[index].Acquire(ctx, 1); err != nil {
			for _, acquiredIndex := range slices.Backward(indexes[:acquired]) {
				s.stripes[acquiredIndex].Release(1)
			}

			return nil, err
		}
		acquired++
	}

	return func() {
		for _, index := range slices.Backward(indexes) {
			s.stripes[index].Release(1)
		}
	}, nil
}

func Index(account domain.AccountKey) int {
	var hash uint64 = 1469598103934665603
	for _, value := range []string{account.LedgerName, account.Account} {
		for i := range len(value) {
			hash ^= uint64(value[i])
			hash *= 1099511628211
		}
		hash ^= 0xff
	}

	return int(hash % stripeCount)
}

func (s *Serializer) TryAcquire(account domain.AccountKey) (func(), bool) {
	stripe := s.stripes[Index(account)]
	if !stripe.TryAcquire(1) {
		return nil, false
	}

	return func() { stripe.Release(1) }, true
}
