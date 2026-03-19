package flightrecorder

import "time"

// Config holds the configuration for the runtime flight recorder.
type Config struct {
	Enabled  bool
	MinAge   time.Duration
	MaxBytes int
}

// DefaultConfig returns the default flight recorder configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:  false,
		MinAge:   5 * time.Second,
		MaxBytes: 10 << 20, // 10 MiB
	}
}
