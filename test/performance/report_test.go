//go:build it

package performance_test

import (
	"github.com/formancehq/go-libs/time"
	"github.com/jamiealquiza/tachymeter"
	"sync"
)

type Result struct {
	Start time.Time
	End   time.Time

	Metrics *tachymeter.Metrics

	Name            string
	Configuration   configuration
	TPS             float64
	InternalMetrics map[string]any
}

type report struct {
	mu *sync.Mutex

	Start time.Time
	End   time.Time

	Tachymeter *tachymeter.Tachymeter

	Scenario        string
	Configuration   configuration
	InternalMetrics map[string]any
}

func (r *report) GetResult() Result {
	return Result{
		Start:           r.Start,
		End:             r.End,
		Metrics:         r.Tachymeter.Calc(),
		InternalMetrics: r.InternalMetrics,
		Name:            r.Scenario,
		Configuration:   r.Configuration,
		TPS:             r.TPS(),
	}
}

func (r *report) TPS() float64 {
	return (float64(time.Duration(r.Tachymeter.Count)) / float64(r.End.Sub(r.Start))) * float64(time.Second)
}

func (r *report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Tachymeter.AddTime(latency)
}

func (r *report) reset() {
	r.Start = time.Now()
	r.Tachymeter.Reset()
}

func newReport(configuration configuration, scenario string) report {
	ret := report{
		Scenario:      scenario,
		Configuration: configuration,
		mu:            &sync.Mutex{},
		Tachymeter: tachymeter.New(&tachymeter.Config{
			Size: 10000,
		}),
	}
	ret.reset()
	return ret
}
