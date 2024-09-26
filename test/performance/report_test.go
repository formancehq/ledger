//go:build it

package performance_test

import (
	"sync"
	"sync/atomic"

	"github.com/formancehq/go-libs/time"
)

type Report struct {
	mu sync.Mutex

	startOfBench time.Time
	endOfBench   time.Time

	transactionsCount int

	longestTransactionID       int
	longestTransactionDuration time.Duration

	totalDuration atomic.Int64

	errors map[int]error
}

func (r *Report) TPS() float64 {
	return (float64(time.Duration(r.transactionsCount)) / float64(r.endOfBench.Sub(r.startOfBench))) * float64(time.Second)
}

func (r *Report) AverageDuration() time.Duration {
	return time.Duration(r.totalDuration.Load()) * time.Millisecond / time.Duration(r.transactionsCount)
}

func (r *Report) LongestTransaction() (int, time.Duration) {
	return r.longestTransactionID, r.longestTransactionDuration
}

func (r *Report) registerTransactionLatency(id int, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.transactionsCount = r.transactionsCount + 1
	if r.longestTransactionDuration < latency {
		r.longestTransactionDuration = latency
		r.longestTransactionID = id
	}
	r.totalDuration.Add(latency.Milliseconds())
	r.errors[id] = err
}

func newReport() *Report {
	return &Report{
		startOfBench: time.Now(),
		errors:       map[int]error{},
	}
}
