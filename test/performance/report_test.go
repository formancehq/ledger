//go:build it

package performance_test

import (
	"encoding/json"
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

func (r Report) MarshalJSON() ([]byte, error) {
	type view struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`

		Metrics *tachymeter.Metrics `json:"metrics"`

		Name              string `json:"name"`
		Configuration     configuration `json:"configuration"`
		TPS float64 `json:"tps"`
	}
	return json.Marshal(view{
		Start:         r.Start,
		End:           r.End,
		Metrics:       r.Tachymeter.Calc(),
		Name:          r.Name,
		Configuration: r.Configuration,
		TPS: r.TPS(),
	})
}

func (r *Report) TPS() float64 {
	return (float64(time.Duration(r.Tachymeter.Count)) / float64(r.End.Sub(r.Start))) * float64(time.Second)
}

func (r *Report) registerTransactionLatency(latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Tachymeter.AddTime(latency)
}

func (r *Report) reset() {
	r.Start = time.Now()
	r.Tachymeter.Reset()
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
