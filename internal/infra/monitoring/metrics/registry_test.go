package metrics_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetricsRegistry verifies that the human-maintained registry in
// misc/devenv/monitoring-dashboards/jsonnet/lib/metrics.libsonnet is
// kept in sync with what the application actually emits.
//
// The dashboard regenerates from that registry, so a drift here is
// silent — panels would lose data and operators would have no
// indication anything is wrong until the dashboard is opened on a
// real cluster. The check therefore runs as a regular Go unit test.
//
// Both directions are enforced:
//   - every metric name in the registry must be created by at least
//     one call site in the codebase;
//   - every instrument our code creates (regardless of meter name)
//     must appear in the registry.
//
// OpenTelemetry semantic-convention auto-instrumentation (go.*,
// process.*, system.*, http.*) targets the *global* MeterProvider —
// our code does not emit those names, so they don't appear here.
func TestMetricsRegistry(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)

	registryPath := filepath.Join(repoRoot, "misc", "devenv", "monitoring-dashboards", "jsonnet", "lib", "metrics.libsonnet")
	registry := parseRegistry(t, registryPath)

	codeNames := collectInstrumentNamesFromCode(t, filepath.Join(repoRoot, "internal"))

	for _, name := range registry {
		require.Contains(t, codeNames, name,
			"metric %q is listed in metrics.libsonnet but no call site emits it — either remove it from the registry or restore the call site",
			name)
	}

	registrySet := make(map[string]struct{}, len(registry))
	for _, n := range registry {
		registrySet[n] = struct{}{}
	}
	for _, n := range codeNames {
		if _, ok := registrySet[n]; !ok {
			t.Errorf("metric %q is emitted in the code but is missing from metrics.libsonnet — add it to the registry so the dashboard can reference it", n)
		}
	}
}

// findRepoRoot walks up from the working directory until it finds
// the repository's go.mod file. Fails the test if no go.mod is
// found.
func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repository root from %s", wd)
		}
		dir = parent
	}
}

// parseRegistry extracts every metric name from the libsonnet
// registry. Names appear as single-quoted string values on the
// right-hand side of a field assignment, e.g.
// `ready: 'bloom.ready',`. Names without a dot are not real OTel
// metric names (an unrelated helper field could legitimately be a
// single word), so the extraction filters to dotted strings only.
func parseRegistry(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	re := regexp.MustCompile(`'([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+)'`)
	matches := re.FindAllStringSubmatch(string(data), -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	sort.Strings(out)

	return out
}

// instrumentMethods are the synchronous and observable instrument
// constructors exposed by go.opentelemetry.io/otel/metric.Meter.
var instrumentMethods = []string{
	"Int64Counter",
	"Int64UpDownCounter",
	"Int64Histogram",
	"Int64Gauge",
	"Int64ObservableCounter",
	"Int64ObservableUpDownCounter",
	"Int64ObservableGauge",
	"Float64Counter",
	"Float64UpDownCounter",
	"Float64Histogram",
	"Float64Gauge",
	"Float64ObservableCounter",
	"Float64ObservableUpDownCounter",
	"Float64ObservableGauge",
}

// collectInstrumentNamesFromCode scans the .go files under root for
// call sites that create instruments and returns the set of unique
// instrument names. Anything our code instantiates is in scope —
// we don't filter by meter name because the naming policy applies
// uniformly to every meter we hand out.
func collectInstrumentNamesFromCode(t *testing.T, root string) []string {
	t.Helper()
	pattern := regexp.MustCompile(
		`\.` + "(" + strings.Join(instrumentMethods, "|") + ")" + `\(\s*"([^"]+)"`,
	)
	seen := make(map[string]struct{})

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// Skip generated mocks: they re-declare the upstream interface
		// methods but never call them.
		if strings.Contains(path, "_generated") || strings.Contains(path, "/mock_") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		matches := pattern.FindAllSubmatch(data, -1)
		for _, m := range matches {
			name := string(m[2])
			if name != "" {
				seen[name] = struct{}{}
			}
		}

		return nil
	})
	require.NoError(t, err)

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)

	return out
}
