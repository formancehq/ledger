// hegel-smoke validates the native runtime without becoming a Composer command.
package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hegel-smoke:", err)
		os.Exit(1)
	}
	fmt.Println("hegel-smoke: PASS (native draws, independent concurrent contexts, error propagation)")
}

func run() error {
	// Each goroutine owns its RunOnce context and callback-local Source.
	const workers = 8
	results := make(chan error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			called := 0
			err := generation.RunOnce(func(s *generation.Source) {
				called++
				for range 32 {
					if n := s.Intn(17); n < 0 || n >= 17 {
						panic("Intn out of bounds")
					}
					if n := s.Int63n(37); n < 0 || n >= 37 {
						panic("Int63n out of bounds")
					}
					if s.Int63() < 0 || s.Uint64Range(math.MaxUint64, math.MaxUint64) != math.MaxUint64 {
						panic("integer boundary mismatch")
					}
					_ = s.Uint64() // Exercise the complete unsigned range.
					if f := s.Float64(); math.IsNaN(f) || f < 0 || f >= 1 {
						panic("Float64 out of bounds")
					}
					if generation.Choice(s, []string{"only"}) != "only" {
						panic("Choice mismatch")
					}
				}
			})
			if called != 1 {
				err = errors.Join(err, fmt.Errorf("callback ran %d times, expected once", called))
			}
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}

	want := errors.New("hegel-smoke callback failure")
	called := 0
	err := generation.RunOnce(func(s *generation.Source) {
		called++
		s.Intn(10)
		panic(want)
	})
	if !errors.Is(err, want) || called != 1 {
		return fmt.Errorf("panic propagation: callback count=%d, error=%w", called, err)
	}

	return nil
}
