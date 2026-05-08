package flightrecorder

import "time"

// Config holds the configuration for the runtime flight recorder.
type Config struct {
	Enabled  bool
	MinAge   time.Duration
	MaxBytes int
}
