package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func sdkPinSources() map[string]string {
	goMod := `require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op
)

replace github.com/antithesishq/antithesis-sdk-go => github.com/formancehq/antithesis-sdk-go v0.0.0-20260915065804-1c9afdaf8204
`
	dockerfile := `RUN git clone --quiet https://github.com/formancehq/antithesis-sdk-go /sdk \
 && git -C /sdk checkout --quiet 1c9afdaf82044c6c7b52d1feb751358034c98506 \
 && go build -o /usr/local/bin/antithesis-go-instrumentor .

# -instrumentor_version sets the SDK version the generated notifier requires.
RUN antithesis-go-instrumentor -instrumentor_version v0.8.0-default-no-op . /instrumented
`

	return map[string]string{
		rootGoMod:      goMod,
		workloadGoMod:  goMod,
		rootDockerfile: dockerfile,
		workloadDocker: dockerfile,
	}
}

func TestSDKPinAcceptsAgreeingPins(t *testing.T) {
	t.Parallel()

	require.Empty(t, checkSDKPinIn(sdkPinSources()))
}

// Each of these is a way the instrumentor and the linked SDK can come apart
// while every build still succeeds.
func TestSDKPinRejectsDrift(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, file, old, new, want string
	}{
		{
			name: "one Dockerfile checks out another commit",
			file: workloadDocker,
			old:  "1c9afdaf82044c6c7b52d1feb751358034c98506",
			new:  "0000000000000000000000000000000000000000",
			want: "fork commit",
		},
		{
			name: "a go.mod replace points at another commit",
			file: workloadGoMod,
			old:  "v0.0.0-20260915065804-1c9afdaf8204",
			new:  "v0.0.0-20260915065804-000000000000",
			want: "fork commit",
		},
		{
			name: "a Dockerfile clones a different fork",
			file: rootDockerfile,
			old:  "https://github.com/formancehq/antithesis-sdk-go /sdk",
			new:  "https://github.com/someone/antithesis-sdk-go /sdk",
			want: "fork module path",
		},
		{
			name: "instrumentor_version falls behind the required version",
			file: rootDockerfile,
			old:  "-instrumentor_version v0.8.0-default-no-op",
			new:  "-instrumentor_version v0.8.0",
			want: "SDK version",
		},
		{
			name: "the two modules require different SDK versions",
			file: workloadGoMod,
			old:  "antithesis-sdk-go v0.8.0-default-no-op",
			new:  "antithesis-sdk-go v0.8.0",
			want: "SDK version",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sources := sdkPinSources()
			require.Contains(t, sources[tc.file], tc.old, "fixture must contain what the case rewrites")
			sources[tc.file] = strings.Replace(sources[tc.file], tc.old, tc.new, 1)

			findings := checkSDKPinIn(sources)
			require.NotEmpty(t, findings)
			require.Contains(t, findings[0].message, "SDK_PIN_DRIFT")
			require.Contains(t, findings[0].message, tc.want)
			// Both sides are named, so the reader does not have to guess which
			// file is the stale one.
			require.Contains(t, findings[0].message, tc.file)
		})
	}
}

// A dropped replace is the failure that silently selects the upstream
// live-by-default SDK, so it must be reported rather than skipped.
func TestSDKPinRejectsAMissingReplace(t *testing.T) {
	t.Parallel()

	sources := sdkPinSources()
	sources[rootGoMod] = strings.Split(sources[rootGoMod], "replace ")[0]

	findings := checkSDKPinIn(sources)
	require.NotEmpty(t, findings)
	require.Contains(t, findings[0].message, "reachable only through this replace")
}
