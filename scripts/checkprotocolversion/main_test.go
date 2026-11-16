package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/require"
)

func TestVerdict(t *testing.T) {
	t.Parallel()

	breaks := []bufFinding{{Path: "misc/proto/api.proto", Type: "FIELD_WIRE_COMPATIBLE_TYPE"}}
	unstopped := []unstoppedChange{{
		from: revisionPoint{version: semver.MustParse("v3.0.0-rc.4"), revision: 3},
		to:   revisionPoint{version: semver.MustParse("v3.0.0-rc.5"), revision: 4, worktree: true},
	}}

	for _, tc := range []struct {
		name     string
		res      result
		errorSub string
	}{
		{name: "unchanged without break", res: result{baseVersion: 3, headVersion: 3}},
		{name: "break without bump", res: result{baseVersion: 3, headVersion: 3, clientFindings: breaks}, errorSub: "1 wire-incompatible change(s) reach a gated service at revision 3"},
		{name: "bump without stop", res: result{baseVersion: 3, headVersion: 4, unstopped: unstopped}, errorSub: "protocol revision 3 at v3.0.0-rc.4 and 4 in this tree (deployable from v3.0.0-rc.5) are not separated by a stopped upgrade"},
		{name: "break with bump without stop", res: result{baseVersion: 3, headVersion: 4, clientFindings: breaks, unstopped: unstopped}, errorSub: "add v3.0.0-rc.5 to mandatoryStopVersions"},
		{name: "unchanged with a stop missing", res: result{baseVersion: 4, headVersion: 4, unstopped: unstopped}, errorSub: "add v3.0.0-rc.5 to mandatoryStopVersions"},
		{name: "bump with stop", res: result{baseVersion: 3, headVersion: 4}},
		{name: "break with bump and stop", res: result{baseVersion: 3, headVersion: 4, clientFindings: breaks}},
		{name: "decrease", res: result{baseVersion: 3, headVersion: 2}, errorSub: `decreased from 3 to 2; restore "3"`},
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

func TestDeployFloor(t *testing.T) {
	t.Parallel()

	for latest, want := range map[string]string{
		"v3.0.0-rc.4":   "v3.0.0-rc.5",
		"v3.0.0-rc.9":   "v3.0.0-rc.10",
		"v3.0.0":        "v3.0.1-0",
		"v3.0.7":        "v3.0.8-0",
		"v3.1.0-0":      "v3.1.0-0",
		"v3.1.0-beta.2": "v3.1.0-beta.2.0",
	} {
		t.Run(latest, func(t *testing.T) {
			t.Parallel()

			floor, err := deployFloor(semver.MustParse(latest))
			require.NoError(t, err)
			require.Equal(t, want, "v"+floor.String())
		})
	}
}

// Every build that can follow a tag is at or above its floor, and the tag is
// below it, so a stop at the floor separates the tag from all of them.
func TestDeployFloorBoundsFollowingBuilds(t *testing.T) {
	t.Parallel()

	for latest, builds := range map[string][]string{
		"v3.0.0-rc.4": {"v3.0.0-rc.5", "v3.0.0", "v3.0.1-dev.1.gabc1234"},
		"v3.0.0":      {"v3.0.1-dev.1.gabc1234", "v3.0.1-beta.1", "v3.0.1-rc.1", "v3.0.1", "v3.1.0-0.dev.1.gabc1234"},
		"v3.1.0-0":    {"v3.1.0-0.dev.1.gabc1234", "v3.1.0-beta.1", "v3.1.0-rc.1", "v3.1.0"},
	} {
		tag := semver.MustParse(latest)
		floor, err := deployFloor(tag)
		require.NoError(t, err)
		require.False(t, floor.LessThan(tag), "%s floor", latest)
		for _, build := range builds {
			require.False(t, semver.MustParse(build).LessThan(floor), "%s after %s", build, latest)
		}
	}
}

func TestUnstoppedChanges(t *testing.T) {
	t.Parallel()

	type point struct {
		version  string
		revision int
	}
	// Listed out of order: git prints rc.10 before rc.9, and tags of several
	// version lines interleave.
	rcLine := []point{{"v3.0.0-rc.10", 41}, {"v3.0.0-rc.9", 40}, {"v3.0.0-rc.1", 40}, {"v3.0.0-rc.11", 41}}
	finalLine := []point{{"v3.0.1-0", 41}, {"v3.0.0", 40}, {"v3.0.0-rc.4", 40}}
	newLine := []point{{"v3.1.0-0", 41}, {"v3.0.2", 40}, {"v3.0.0", 40}, {"v3.0.1", 40}}

	for _, tc := range []struct {
		name   string
		points []point
		stops  []string
		want   []string // "from to" pairs
	}{
		{name: "same revision needs no stop", points: []point{{"v3.0.0-rc.5", 40}, {"v3.0.0-rc.4", 40}}},
		{name: "stop at the first release on the new revision", points: rcLine, stops: []string{"v3.0.0-rc.10"}},
		{name: "released stop below the change", points: rcLine, stops: []string{"v3.0.0-rc.9"}, want: []string{"v3.0.0-rc.9 v3.0.0-rc.10"}},
		{name: "stop only at a later rc", points: rcLine, stops: []string{"v3.0.0-rc.11"}, want: []string{"v3.0.0-rc.9 v3.0.0-rc.10"}},
		// rc.10 can ship before the final release and would roll.
		{name: "stop only at the final release", points: rcLine, stops: []string{"v3.0.0"}, want: []string{"v3.0.0-rc.9 v3.0.0-rc.10"}},
		{name: "no stop", points: rcLine, want: []string{"v3.0.0-rc.9 v3.0.0-rc.10"}},
		{name: "stop at the floor after a final release", points: finalLine, stops: []string{"v3.0.1-0"}},
		// A dev or beta build of v3.0.1 sorts below rc.1 and would roll.
		{name: "stop only at the patch rc", points: finalLine, stops: []string{"v3.0.1-rc.1"}, want: []string{"v3.0.0 v3.0.1-0"}},
		{name: "stop at the marker of a new line", points: newLine, stops: []string{"v3.1.0-0"}},
		{name: "stop only on the old line", points: newLine, stops: []string{"v3.0.1"}, want: []string{"v3.0.2 v3.1.0-0"}},
		{
			name:   "every change needs its own stop",
			points: []point{{"v3.0.0-rc.4", 40}, {"v3.0.0-rc.5", 41}, {"v3.0.0", 42}, {"v3.0.1-0", 41}},
			stops:  []string{"v3.0.0-rc.5"},
			want:   []string{"v3.0.0-rc.5 v3.0.0", "v3.0.0 v3.0.1-0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			points := make([]revisionPoint, len(tc.points))
			for i, p := range tc.points {
				points[i] = revisionPoint{version: semver.MustParse(p.version), revision: p.revision}
			}
			stops := make([]*semver.Version, len(tc.stops))
			for i, stop := range tc.stops {
				stops[i] = semver.MustParse(stop)
			}

			var got []string
			for _, change := range unstoppedChanges(points, stops) {
				got = append(got, "v"+change.from.version.String()+" v"+change.to.version.String())
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseTag(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]bool{
		"v3.0.0":        true,
		"v3.0.0-rc.4":   true,
		"v3.1.0-0":      true,
		"v3.0.0-beta.1": true,
		"3.0.0":         false,
		"v3.0":          false,
		"v3.0.0+build":  false,
		"v03.0.0":       false,
		"nightly":       false,
	} {
		_, ok := parseTag(name)
		require.Equal(t, want, ok, name)
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
			name:      "gated retype with bump and next-release stop",
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
			errorSub:  "protocol revision 3 at v3.0.0-rc.4 and 4 in this tree (deployable from v3.0.0-rc.5) are not separated by a stopped upgrade",
		},
		{
			name:      "gated retype with bump and final stop only",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "4",
			stops:     []string{"v3.0.0"},
			wantGated: 1,
			errorSub:  "add v3.0.0-rc.5 to mandatoryStopVersions",
		},
		{
			name:      "gated retype with bump and later stop only",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "4",
			stops:     []string{"v3.0.0-rc.4", "v3.0.0-rc.6"},
			wantGated: 1,
			errorSub:  "add v3.0.0-rc.5 to mandatoryStopVersions",
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

func TestCheckerRequiresReachableVersionTag(t *testing.T) {
	t.Parallel()

	repo := newFixtureRepo(t)
	runGit(t, repo, "tag", "--delete", "v3.0.0-rc.4")

	_, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
	require.ErrorContains(t, err, "no version tag is reachable from HEAD")
}

func TestCheckerRejectsNonCanonicalStop(t *testing.T) {
	t.Parallel()

	repo := newFixtureRepo(t)
	writeFixture(t, repo, stopFile, stopSource([]string{"3.0.0-rc.5"}))

	_, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
	require.ErrorContains(t, err, `mandatoryStopVersions entry "3.0.0-rc.5" is not a canonical version`)
}

// The stop is checked against the released revisions on every run, whether
// or not the checked change touches the revision.
func TestCheckerChecksReleasedRevisions(t *testing.T) {
	t.Parallel()

	// bumped returns a repository whose base commit raised the revision to 4
	// after v3.0.0-rc.4 with a stop at v3.0.0-rc.5.
	bumped := func(t *testing.T) string {
		t.Helper()

		repo := newFixtureRepo(t)
		writeFixture(t, repo, protocolFile, protocolSource("4"))
		writeFixture(t, repo, stopFile, stopSource([]string{"v3.0.0-rc.5"}))
		commitFixture(t, repo, "bump")

		return repo
	}

	for _, tc := range []struct {
		name     string
		arrange  func(t *testing.T, repo string)
		errorSub string
	}{
		{name: "stop kept", arrange: func(*testing.T, string) {}},
		{
			name: "stop removed without touching the revision",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				writeFixture(t, repo, stopFile, stopSource(nil))
			},
			errorSub: "protocol revision 3 at v3.0.0-rc.4 and 4 in this tree (deployable from v3.0.0-rc.5) are not separated by a stopped upgrade",
		},
		{
			name: "stop still required once its release is tagged",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "v3.0.0-rc.5")
				writeFixture(t, repo, stopFile, stopSource(nil))
			},
			errorSub: "protocol revision 3 at v3.0.0-rc.4 and 4 at v3.0.0-rc.5 are not separated by a stopped upgrade",
		},
		{
			name: "release tagged before the bump moves the stop",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "v3.0.0-rc.5", "HEAD~1")
			},
			errorSub: "protocol revision 3 at v3.0.0-rc.5 and 4 in this tree (deployable from v3.0.0-rc.6) are not separated by a stopped upgrade",
		},
		{
			name: "release tagged on another branch",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "v3.1.0-0")
				runGit(t, repo, "checkout", "--quiet", "-b", "release", "HEAD~1")
				writeFixture(t, repo, "RELEASE", "v3.0.0\n")
				commitFixture(t, repo, "release")
				runGit(t, repo, "tag", "v3.0.0")
				runGit(t, repo, "checkout", "--quiet", "-")
			},
			errorSub: "protocol revision 3 at v3.0.0 and 4 in this tree (deployable from v3.1.0-0) are not separated by a stopped upgrade",
		},
		{
			name: "release before the baseline is ignored",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "checkout", "--quiet", "-b", "beta", "HEAD~1")
				writeFixture(t, repo, protocolFile, protocolSource("2"))
				commitFixture(t, repo, "beta")
				runGit(t, repo, "tag", "v3.0.0-beta.9")
				runGit(t, repo, "checkout", "--quiet", "-")
			},
		},
		{
			// The marker sits on the branch point, before the bump.
			name: "marker is not a release",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "v3.1.0-0", "HEAD~1")
				runGit(t, repo, "tag", "v3.1.0-rc.1")
				writeFixture(t, repo, stopFile, stopSource([]string{"v3.1.0-0"}))
			},
		},
		{
			name: "release above this branch is ignored",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "checkout", "--quiet", "-b", "next")
				writeFixture(t, repo, protocolFile, protocolSource("5"))
				commitFixture(t, repo, "next")
				runGit(t, repo, "tag", "v3.1.0-rc.1")
				runGit(t, repo, "checkout", "--quiet", "-")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := bumped(t)
			tc.arrange(t, repo)

			res, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
			require.NoError(t, err)
			require.Equal(t, res.baseVersion, res.headVersion)

			err = res.verdict()
			if tc.errorSub == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.errorSub)
		})
	}
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
			commitFixture(t, repo, "compact base")
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
	writeFixture(t, repo, stopFile, stopSource(nil))
	runGit(t, repo, "init", "--quiet")
	commitFixture(t, repo, "base")
	runGit(t, repo, "tag", "v3.0.0-rc.4")

	return repo
}

func commitFixture(t *testing.T, repo, message string) {
	t.Helper()

	runGit(t, repo, "add", ".")
	runGit(t, repo, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", message)
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
