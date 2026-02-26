package worker

// DrainChannel reads items from ch and calls process for each one until stop
// is closed. It is the standard loop for channel-based workers.
func DrainChannel[T any](stop <-chan struct{}, ch <-chan T, process func(T)) {
	for {
		select {
		case <-stop:
			return
		case req := <-ch:
			process(req)
		}
	}
}
