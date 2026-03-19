//go:build pyroscope

package server

import (
	"strings"
	"time"

	gopyscope "github.com/grafana/pyroscope-go"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"github.com/formancehq/go-libs/v4/collectionutils"

	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/pyroscope"
)

const (
	// PyroscopeEnabledFlag enables Pyroscope profiling.
	PyroscopeEnabledFlag = "pyroscope-enabled"

	// PyroscopeServerAddressFlag sets the Pyroscope server address.
	PyroscopeServerAddressFlag = "pyroscope-server-address"

	// PyroscopeApplicationNameFlag sets the application name in Pyroscope.
	PyroscopeApplicationNameFlag = "pyroscope-application-name"

	// PyroscopeAuthTokenFlag sets the authentication token for Pyroscope.
	PyroscopeAuthTokenFlag = "pyroscope-auth-token"

	// PyroscopeTenantIDFlag sets the tenant ID for multi-tenant Pyroscope.
	PyroscopeTenantIDFlag = "pyroscope-tenant-id"

	// PyroscopeBasicAuthUserFlag sets the basic auth username.
	PyroscopeBasicAuthUserFlag = "pyroscope-basic-auth-user"

	// PyroscopeBasicAuthPasswordFlag sets the basic auth password.
	PyroscopeBasicAuthPasswordFlag = "pyroscope-basic-auth-password"

	// PyroscopeUploadRateFlag sets the profile upload rate.
	PyroscopeUploadRateFlag = "pyroscope-upload-rate"

	// PyroscopeTagsFlag sets additional tags for profiles (format: key=value).
	PyroscopeTagsFlag = "pyroscope-tags"

	// PyroscopeProfileTypesFlag sets which profile types to enable.
	PyroscopeProfileTypesFlag = "pyroscope-profile-types"

	// PyroscopeMutexProfileFractionFlag sets the mutex profile fraction.
	PyroscopeMutexProfileFractionFlag = "pyroscope-mutex-profile-fraction"

	// PyroscopeBlockProfileRateFlag sets the block profile rate.
	PyroscopeBlockProfileRateFlag = "pyroscope-block-profile-rate"

	// PyroscopeDisableGCRunsFlag disables automatic GC runs between heap profiles.
	PyroscopeDisableGCRunsFlag = "pyroscope-disable-gc-runs"
)

// addPyroscopeFlags adds Pyroscope-related flags to the given flag set.
func addPyroscopeFlags(flags *flag.FlagSet) {
	flags.Bool(PyroscopeEnabledFlag, false, "Enable Pyroscope continuous profiling")
	flags.String(PyroscopeServerAddressFlag, "http://localhost:4040", "Pyroscope server address")
	flags.String(PyroscopeApplicationNameFlag, "", "Application name for Pyroscope (defaults to service name)")
	flags.String(PyroscopeAuthTokenFlag, "", "Authentication token for Pyroscope (for Grafana Cloud)")
	flags.String(PyroscopeTenantIDFlag, "", "Tenant ID for multi-tenant Pyroscope (for Grafana Cloud)")
	flags.String(PyroscopeBasicAuthUserFlag, "", "Basic auth username for Pyroscope")
	flags.String(PyroscopeBasicAuthPasswordFlag, "", "Basic auth password for Pyroscope")
	flags.Duration(PyroscopeUploadRateFlag, 15*time.Second, "Profile upload rate")
	flags.StringSlice(PyroscopeTagsFlag, []string{}, "Additional tags for profiles (format: key=value, can be specified multiple times)")
	flags.StringSlice(PyroscopeProfileTypesFlag, collectionutils.Keys(allProfiles),
		"Profile types to enable (cpu,alloc_objects,alloc_space,inuse_objects,inuse_space,goroutines,mutex_count,mutex_duration,block_count,block_duration)")
	flags.Int(PyroscopeMutexProfileFractionFlag, 5, "Mutex profile fraction (0 to disable)")
	flags.Int(PyroscopeBlockProfileRateFlag, 5, "Block profile rate (0 to disable)")
	flags.Bool(PyroscopeDisableGCRunsFlag, false, "Disable automatic GC runs between heap profile uploads")
}

// pyroscopeConfigFromFlags creates a pyroscope.Config from command flags.
func pyroscopeConfigFromFlags(cmd *cobra.Command) pyroscope.Config {
	cfg := pyroscope.DefaultConfig()

	cfg.Enabled, _ = cmd.Flags().GetBool(PyroscopeEnabledFlag)
	cfg.ServerAddress, _ = cmd.Flags().GetString(PyroscopeServerAddressFlag)
	cfg.ApplicationName, _ = cmd.Flags().GetString(PyroscopeApplicationNameFlag)
	cfg.AuthToken, _ = cmd.Flags().GetString(PyroscopeAuthTokenFlag)
	cfg.TenantID, _ = cmd.Flags().GetString(PyroscopeTenantIDFlag)
	cfg.BasicAuthUser, _ = cmd.Flags().GetString(PyroscopeBasicAuthUserFlag)
	cfg.BasicAuthPassword, _ = cmd.Flags().GetString(PyroscopeBasicAuthPasswordFlag)
	cfg.UploadRate, _ = cmd.Flags().GetDuration(PyroscopeUploadRateFlag)
	cfg.MutexProfileFraction, _ = cmd.Flags().GetInt(PyroscopeMutexProfileFractionFlag)
	cfg.BlockProfileRate, _ = cmd.Flags().GetInt(PyroscopeBlockProfileRateFlag)
	cfg.DisableGCRuns, _ = cmd.Flags().GetBool(PyroscopeDisableGCRunsFlag)

	// Parse tags
	tagsSlice, _ := cmd.Flags().GetStringSlice(PyroscopeTagsFlag)
	if len(tagsSlice) > 0 {
		cfg.Tags = parsePyroscopeTags(tagsSlice)
	}

	// Parse profile types
	profileTypesSlice, _ := cmd.Flags().GetStringSlice(PyroscopeProfileTypesFlag)
	if len(profileTypesSlice) > 0 {
		cfg.ProfileTypes = parsePyroscopeProfileTypes(profileTypesSlice)
	}

	return cfg
}

// parsePyroscopeTags parses a slice of key=value pairs into a map.
func parsePyroscopeTags(tagsSlice []string) map[string]string {
	tags := make(map[string]string)

	for _, pair := range tagsSlice {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])

			value := strings.TrimSpace(parts[1])
			if key != "" {
				tags[key] = value
			}
		}
	}

	return tags
}

var allProfiles = map[string]gopyscope.ProfileType{
	"cpu":            gopyscope.ProfileCPU,
	"alloc_objects":  gopyscope.ProfileAllocObjects,
	"alloc_space":    gopyscope.ProfileAllocSpace,
	"inuse_objects":  gopyscope.ProfileInuseObjects,
	"inuse_space":    gopyscope.ProfileInuseSpace,
	"goroutines":     gopyscope.ProfileGoroutines,
	"mutex_count":    gopyscope.ProfileMutexCount,
	"mutex_duration": gopyscope.ProfileMutexDuration,
	"block_count":    gopyscope.ProfileBlockCount,
	"block_duration": gopyscope.ProfileBlockDuration,
}

// parsePyroscopeProfileTypes parses a slice of profile type names.
func parsePyroscopeProfileTypes(typesSlice []string) []gopyscope.ProfileType {
	var types []gopyscope.ProfileType

	for _, name := range typesSlice {
		name = strings.TrimSpace(strings.ToLower(name))
		if pt, ok := allProfiles[name]; ok {
			types = append(types, pt)
		}
	}

	return types
}
