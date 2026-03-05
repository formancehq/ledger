//go:build !pyroscope

package pyroscope

// Config is a stub for builds without the pyroscope tag.
type Config struct {
	Enabled         bool
	ApplicationName string
	Tags            map[string]string
}

// DefaultConfig returns a no-op default configuration.
func DefaultConfig() Config {
	return Config{}
}
