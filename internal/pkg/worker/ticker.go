package worker

import "time"

// RunTicker calls fn at the given interval until stop is closed. The ticker is
// cleaned up when RunTicker returns.
func RunTicker(stop <-chan struct{}, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fn()
		}
	}
}
