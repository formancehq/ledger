// Package version exposes the build metadata of the binary. The exported
// vars are set at build time via -ldflags -X; they default to dev values.
package version

import (
	"runtime"
	"strings"
)

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

// ServiceVersion formats the build for the OpenTelemetry service.version
// resource attribute as semver build metadata, <version>+<commit>. Build
// metadata does not affect semver precedence, whereas <version>-<commit>
// reads as a pre-release ordered before <version>. The commit is omitted
// when it is unknown or already part of the version, as in goreleaser
// snapshot versions, which embed the short commit.
func (i Info) ServiceVersion() string {
	if i.Commit == "" || i.Commit == "unknown" || strings.Contains(i.Version, i.Commit) {
		return i.Version
	}

	return i.Version + "+" + i.Commit
}
