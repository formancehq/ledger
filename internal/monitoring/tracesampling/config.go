package tracesampling

// Config holds the configuration for trace sampling.
type Config struct {
	// Enabled indicates whether error-aware sampling is enabled.
	// When disabled, all traces are exported (default OpenTelemetry behavior).
	Enabled bool

	// SuccessRatio is the sampling ratio for successful spans (0.0-1.0).
	// 0.0 means no successful spans are sampled.
	// 1.0 means all successful spans are sampled.
	// Error spans are always sampled regardless of this setting.
	SuccessRatio float64
}

// DefaultConfig returns the default trace sampling configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:      false,
		SuccessRatio: 1.0, // By default, sample all successful spans
	}
}
