package pyroscope

import (
	"runtime"
	"time"

	"github.com/grafana/pyroscope-go"
)

// Config holds the configuration for Pyroscope profiling.
type Config struct {
	// Enabled indicates whether Pyroscope profiling is enabled.
	Enabled bool

	// ServerAddress is the Pyroscope server address (e.g., http://localhost:4040).
	ServerAddress string

	// ApplicationName is the name of the application to identify in Pyroscope.
	ApplicationName string

	// Tags are additional labels to attach to all profiles.
	Tags map[string]string

	// AuthToken is the authentication token for Pyroscope (used with Grafana Cloud).
	AuthToken string

	// TenantID is the tenant ID for multi-tenant Pyroscope (used with Grafana Cloud).
	TenantID string

	// BasicAuthUser is the basic auth username (used with Grafana Cloud).
	BasicAuthUser string

	// BasicAuthPassword is the basic auth password (used with Grafana Cloud).
	BasicAuthPassword string

	// UploadRate is the rate at which profiles are uploaded.
	UploadRate time.Duration

	// ProfileTypes specifies which profile types to enable.
	ProfileTypes []pyroscope.ProfileType

	// MutexProfileFraction controls the fraction of mutex contention events reported.
	// See runtime.SetMutexProfileFraction.
	MutexProfileFraction int

	// BlockProfileRate controls the fraction of goroutine blocking events reported.
	// See runtime.SetBlockProfileRate.
	BlockProfileRate int

	// DisableGCRuns disables automatic GC runs between heap profile uploads.
	DisableGCRuns bool
}

// DefaultConfig returns the default Pyroscope configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:              false,
		ServerAddress:        "http://localhost:4040",
		ApplicationName:      "ledger-v3-poc",
		Tags:                 make(map[string]string),
		UploadRate:           15 * time.Second,
		MutexProfileFraction: 5,
		BlockProfileRate:     5,
		DisableGCRuns:        false,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	}
}

// PyroscopeConfig converts the Config to pyroscope.Config.
func (c *Config) PyroscopeConfig() pyroscope.Config {
	cfg := pyroscope.Config{
		ApplicationName:   c.ApplicationName,
		ServerAddress:     c.ServerAddress,
		Tags:              c.Tags,
		ProfileTypes:      c.ProfileTypes,
		DisableGCRuns:     c.DisableGCRuns,
		UploadRate:        c.UploadRate,
		AuthToken:         c.AuthToken,
		TenantID:          c.TenantID,
		BasicAuthUser:     c.BasicAuthUser,
		BasicAuthPassword: c.BasicAuthPassword,
	}

	return cfg
}

// SetupRuntimeProfiling configures Go runtime profiling rates.
func (c *Config) SetupRuntimeProfiling() {
	if c.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(c.MutexProfileFraction)
	}
	if c.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(c.BlockProfileRate)
	}
}
