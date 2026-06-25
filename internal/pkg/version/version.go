// Package version exposes the build metadata of the binary. The exported
// vars are set at build time via -ldflags -X; they default to dev values.
package version

import "runtime"

// Build metadata, set via:
//
//	-ldflags "-X github.com/formancehq/ledger/v3/internal/pkg/version.Version=v3.1.0 ..."
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// Info is the build metadata exposed to clients over HTTP and gRPC.
type Info struct {
	Version   string
	Commit    string
	BuildDate string
	GoVersion string
}

// Get returns the build metadata, filling GoVersion from the runtime.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
	}
}
