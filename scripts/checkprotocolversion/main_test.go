package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerdict(t *testing.T) {
	t.Parallel()

	breaks := []bufFinding{{Path: "misc/proto/api.proto", Type: "FIELD_WIRE_COMPATIBLE_TYPE"}}
	pending := []string{"v3.0.0-rc.5"}

	for _, tc := range []struct {
		name     string
		res      result
		errorSub string
	}{
		{name: "unchanged without break", res: result{baseVersion: 3, headVersion: 3}},
		{name: "break without bump", res: result{baseVersion: 3, headVersion: 3, clientFindings: breaks, pendingStops: pending}, errorSub: "1 wire-incompatible change(s) reach a gated service at revision 3"},
		{name: "bump without pending stop", res: result{baseVersion: 3, headVersion: 4}, errorSub: "changed from 3 to 4 without a pending stopped upgrade"},
		{name: "break with bump without pending stop", res: result{baseVersion: 3, headVersion: 4, clientFindings: breaks}, errorSub: "without a pending stopped upgrade"},
		{name: "bump with pending stop", res: result{baseVersion: 3, headVersion: 4, pendingStops: pending}},
		{name: "break with bump and pending stop", res: result{baseVersion: 3, headVersion: 4, clientFindings: breaks, pendingStops: pending}},
		{name: "decrease", res: result{baseVersion: 3, headVersion: 2, pendingStops: pending}, errorSub: `decreased from 3 to 2; restore "3"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.res.verdict()
			if tc.errorSub == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.errorSub)
		})
	}
}

func TestParseVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		source   string
		want     int
		errorSub string
	}{
		{name: "grouped const", source: "package p\nconst (\n\tVersion = \"41\"\n\tMetadataKey = \"k\"\n)\n", want: 41},
		{name: "leading zero", source: "package p\nconst Version = \"041\"\n", errorSub: "canonical"},
		{name: "zero", source: "package p\nconst Version = \"0\"\n", errorSub: "canonical"},
		{name: "not a literal", source: "package p\nconst Version = other\n", errorSub: "string literal"},
		{name: "missing", source: "package p\nconst Other = \"1\"\n", errorSub: "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseVersion([]byte(tc.source))
			if tc.errorSub != "" {
				require.ErrorContains(t, err, tc.errorSub)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseStopVersions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		source   string
		want     []string
		errorSub string
	}{
		{name: "entries", source: "package p\nvar mandatoryStopVersions = []string{\"v3.0.0-rc.4\", \"v3.0.0-rc.5\"}\n", want: []string{"v3.0.0-rc.4", "v3.0.0-rc.5"}},
		{name: "empty", source: "package p\nvar mandatoryStopVersions = []string{}\n", want: []string{}},
		{name: "grouped var", source: "package p\nvar (\n\tother = 1\n\tmandatoryStopVersions = []string{\"v1.0.0\"}\n)\n", want: []string{"v1.0.0"}},
		{name: "computed", source: "package p\nvar mandatoryStopVersions = versions()\n", errorSub: "[]string literal"},
		{name: "array", source: "package p\nvar mandatoryStopVersions = [1]string{\"v1.0.0\"}\n", errorSub: "[]string literal"},
		{name: "non-literal entry", source: "package p\nvar mandatoryStopVersions = []string{other}\n", errorSub: "string literals"},
		{name: "missing", source: "package p\nvar other = []string{}\n", errorSub: "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseStopVersions([]byte(tc.source))
			if tc.errorSub != "" {
				require.ErrorContains(t, err, tc.errorSub)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// The operator's declaration must stay readable by the gate.
func TestParseStopVersionsFromOperator(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join(repoRoot(t), stopFile))
	require.NoError(t, err)

	stops, err := parseStopVersions(source)
	require.NoError(t, err)
	require.NotEmpty(t, stops)
}

const fixtureProto = `syntax = "proto3";

package api;

service Api {
  rpc Get(GetRequest) returns (GetResponse);
}

message GetRequest {
  string id = 1;
}

message GetResponse {
  Inner inner = 1;
}

message Inner {
  string label = 1;
  int64 amount = 2;
  int32 count = 3;
}

message RaftOnly {
  string key = 1;
  int64 term = 2;
}
`

// TestCheckerClassifiesAgainstBase drives the real git and buf binaries on a
// throwaway repository, so the classification is exercised end to end.
func TestCheckerClassifiesAgainstBase(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		proto        string
		version      string
		stops        []string
		wantGated    int
		wantInternal int
		errorSub     string
	}{
		{
			name:    "no contract change",
			proto:   fixtureProto,
			version: "3",
		},
		{
			name:      "gated retype",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "3",
			wantGated: 1,
			errorSub:  "1 wire-incompatible change(s) reach a gated service",
		},
		{
			name:      "gated retype with bump and pending stop",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "4",
			stops:     []string{"v3.0.0-rc.4", "v3.0.0-rc.5"},
			wantGated: 1,
		},
		{
			name:      "gated retype with bump and released stop only",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "4",
			stops:     []string{"v3.0.0-rc.4"},
			wantGated: 1,
			errorSub:  "changed from 3 to 4 without a pending stopped upgrade",
		},
		{
			name:      "gated field deletion",
			proto:     strings.Replace(fixtureProto, "  int64 amount = 2;\n", "", 1),
			version:   "3",
			wantGated: 1,
			errorSub:  "wire-incompatible",
		},
		{
			name:    "gated field addition",
			proto:   strings.Replace(fixtureProto, "int32 count = 3;", "int32 count = 3;\n  bool settled = 4;", 1),
			version: "3",
		},
		{
			// Same varint wire type: existing peers decode the value unchanged.
			name:    "gated int32 to int64 widening",
			proto:   strings.Replace(fixtureProto, "int32 count = 3;", "int64 count = 3;", 1),
			version: "3",
		},
		{
			name:         "internal-only break",
			proto:        strings.Replace(fixtureProto, "int64 term = 2;", "string term = 2;", 1),
			version:      "3",
			wantInternal: 1,
		},
		{
			name:      "removed rpc",
			proto:     strings.Replace(fixtureProto, "  rpc Get(GetRequest) returns (GetResponse);\n", "", 1),
			version:   "3",
			wantGated: 1,
			errorSub:  "wire-incompatible",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := newFixtureRepo(t)
			runGit(t, repo, "tag", "v3.0.0-rc.4")
			writeFixture(t, repo, "misc/proto/api.proto", tc.proto)
			writeFixture(t, repo, protocolFile, protocolSource(tc.version))
			writeFixture(t, repo, stopFile, stopSource(tc.stops))

			res, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
			require.NoError(t, err)
			require.Equal(t, 3, res.baseVersion)
			require.Len(t, res.clientFindings, tc.wantGated)
			require.Equal(t, tc.wantInternal, res.otherFindings)

			err = res.verdict()
			if tc.errorSub == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.errorSub)
		})
	}
}

func TestCheckerRequiresReleaseTagsForBump(t *testing.T) {
	t.Parallel()

	repo := newFixtureRepo(t)
	writeFixture(t, repo, protocolFile, protocolSource("4"))
	writeFixture(t, repo, stopFile, stopSource([]string{"v3.0.0-rc.5"}))

	_, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
	require.ErrorContains(t, err, "no release tags found")
}

func TestCheckerRejectsUnknownGatedService(t *testing.T) {
	t.Parallel()

	repo := newFixtureRepo(t)

	_, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Missing"}, pluginRoot: repoRoot(t)}.run()
	require.ErrorContains(t, err, "gated service api.Missing is not declared")
}

func TestCheckerClassifiesSameLineNestedMessages(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		requestType  string
		wantGated    int
		wantInternal int
	}{
		{name: "nested message is gated", requestType: "Outer.Inner", wantGated: 1},
		{name: "nested message is internal", requestType: "Outer", wantInternal: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := newFixtureRepo(t)
			baseProto := `syntax = "proto3";
package api;
service Api { rpc Get(` + tc.requestType + `) returns (GetResponse); }
message Outer { message Inner { string value = 1; } }
message GetResponse { string result = 1; }
`
			writeFixture(t, repo, "misc/proto/api.proto", baseProto)
			runGit(t, repo, "add", ".")
			runGit(t, repo, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "compact base")
			writeFixture(t, repo, "misc/proto/api.proto", strings.Replace(baseProto, "string value = 1;", "int64 value = 1;", 1))

			res, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
			require.NoError(t, err)
			require.Len(t, res.clientFindings, tc.wantGated)
			require.Equal(t, tc.wantInternal, res.otherFindings)
			if tc.wantGated > 0 {
				require.ErrorContains(t, res.verdict(), "wire-incompatible change(s) reach a gated service")
			} else {
				require.NoError(t, res.verdict())
			}
		})
	}
}

func newFixtureRepo(t *testing.T) string {
	t.Helper()

	repo := t.TempDir()
	config, err := os.ReadFile(filepath.Join(repoRoot(t), bufConfig))
	require.NoError(t, err)

	writeFixture(t, repo, bufConfig, string(config))
	writeFixture(t, repo, "misc/proto/api.proto", fixtureProto)
	writeFixture(t, repo, protocolFile, protocolSource("3"))
	runGit(t, repo, "init", "--quiet")
	runGit(t, repo, "add", ".")
	runGit(t, repo, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "base")

	return repo
}

func stopSource(stops []string) string {
	quoted := make([]string, len(stops))
	for i, stop := range stops {
		quoted[i] = strconv.Quote(stop)
	}

	return "package controller\n\nvar mandatoryStopVersions = []string{" + strings.Join(quoted, ", ") + "}\n"
}

func protocolSource(version string) string {
	return "package grpcprotocol\n\nconst (\n\tVersion = \"" + version + "\"\n)\n"
}

func writeFixture(t *testing.T, repo, path, content string) {
	t.Helper()

	full := filepath.Join(repo, filepath.FromSlash(path))
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
}

func runGit(t *testing.T, repo string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func repoRoot(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}
