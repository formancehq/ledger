package internal

import "log"

// LogCleanupError reports a best-effort cleanup failure. A final transient
// error is always possible depending on the level of chaos and the retry
// configuration, so transient errors are tagged as expected; anything else is
// surfaced as unexpected so it can be triaged from the run logs.
func LogCleanupError(operation string, err error) {
	if err == nil {
		return
	}

	if IsTransient(err) {
		log.Printf("cleanup: %s failed (transient, expected under faults): %s", operation, err)

		return
	}

	log.Printf("cleanup: %s failed unexpectedly: %s", operation, err)
}
