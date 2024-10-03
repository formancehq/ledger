//go:build it

package performance_test

import (
	"sync"
	"sync/atomic"

	"github.com/formancehq/go-libs/time"
)

type Report struct {
	mu sync.Mutex

	features      map[string]string

	startOfBench time.Time
	endOfBench   time.Time

	totalDuration atomic.Int64
	transactionsCount int
}

func (r *Report) TPS() float64 {
	return (float64(time.Duration(r.transactionsCount)) / float64(r.endOfBench.Sub(r.startOfBench))) * float64(time.Second)
}

func (r *Report) AverageDuration() time.Duration {
	return time.Duration(r.totalDuration.Load()) * time.Millisecond / time.Duration(r.transactionsCount)
}

func (r *Report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.transactionsCount++
	r.totalDuration.Add(latency.Milliseconds())
}

func newReport(features map[string]string) *Report {
	return &Report{
		startOfBench: time.Now(),
		features: features,
	}
}
