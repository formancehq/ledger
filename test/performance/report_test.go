//go:build it

package performance_test

import (
	"sync"
	"sync/atomic"

	"github.com/formancehq/go-libs/time"
)

type Report struct {
	mu *sync.Mutex


	Start time.Time
	End   time.Time

	TotalLatency      *atomic.Int64
	TransactionsCount int
	Name              string
	Configuration     configuration
}

func (r *Report) TPS() float64 {
	return (float64(time.Duration(r.TransactionsCount)) / float64(r.End.Sub(r.Start))) * float64(time.Second)
}

func (r *Report) AverageDuration() time.Duration {
	return time.Duration(r.TotalLatency.Load()) * time.Millisecond / time.Duration(r.TransactionsCount)
}

func (r *Report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.TransactionsCount++
	r.TotalLatency.Add(latency.Milliseconds())
}

func (r *Report) reset() {
	r.TotalLatency = &atomic.Int64{}
	r.TransactionsCount = 0
	r.Start = time.Now()
}

func newReport(configuration configuration, name string) Report {
	ret := Report{
		Name:          name,
		Configuration: configuration,
		mu:            &sync.Mutex{},
	}
	ret.reset()
	return ret
}
