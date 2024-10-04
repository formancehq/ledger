//go:build it

package performance_test

import (
	"github.com/formancehq/go-libs/time"
	"github.com/jamiealquiza/tachymeter"
	"sync"
)

type Report struct {
	mu *sync.Mutex

	Start time.Time
	End   time.Time

	Tachymeter *tachymeter.Tachymeter

	Name              string
	Configuration     configuration
}

func (r *Report) TPS() float64 {
	return (float64(time.Duration(r.Tachymeter.Count)) / float64(r.End.Sub(r.Start))) * float64(time.Second)
}

func (r *Report) AverageDuration() time.Duration {
	return r.Tachymeter.Calc().Time.Avg
}

func (r *Report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Tachymeter.AddTime(latency)
}

func (r *Report) reset() {
	r.Start = time.Now()
}

func newReport(configuration configuration, name string) Report {
	ret := Report{
		Name:          name,
		Configuration: configuration,
		mu:            &sync.Mutex{},
		Tachymeter: tachymeter.New(&tachymeter.Config{
			Size: 10000,
		}),
	}
	ret.reset()
	return ret
}
