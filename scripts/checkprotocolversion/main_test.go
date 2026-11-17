package main

import (
	"cmp"
	"encoding/json"
	"errors"
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

	for _, tc := range []struct {
		name     string
		res      result
		errorSub string
	}{
		{name: "unchanged without break", res: result{baseVersion: 3, headVersion: 3}},
		{name: "break", res: result{baseVersion: 3, headVersion: 3, clientFindings: breaks}, errorSub: "1 wire-incompatible change(s) reach a gated service"},
		{name: "bump without break", res: result{baseVersion: 3, headVersion: 4}, errorSub: "changed from 3 to 4"},
		{name: "bump does not excuse a break", res: result{baseVersion: 3, headVersion: 4, clientFindings: breaks}, errorSub: "wire-incompatible"},
		{name: "exception stop error", res: result{baseVersion: 3, headVersion: 3, excepted: 1, exceptionErrors: []error{errors.New("stop not listed")}}, errorSub: "stop not listed"},
		{name: "exception with its stop", res: result{baseVersion: 3, headVersion: 3, excepted: 1}},
		{name: "decrease", res: result{baseVersion: 3, headVersion: 2}, errorSub: `changed from 3 to 2; it is frozen during the v3 release-candidate phase: restore "3"`},
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
			name:      "gated retype with bump",
			proto:     strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1),
			version:   "4",
			wantGated: 1,
			errorSub:  "changed from 3 to 4",
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

func TestOnReleaseLine(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		baseRef string
		tag     string
		want    bool
	}{
		{baseRef: "", tag: "v3.1.0-0", want: true},
		{baseRef: "main", tag: "v3.1.0-0", want: true},
		{baseRef: "release/v3.0", tag: "v3.0.0-rc.4", want: true},
		{baseRef: "refs/heads/release/v3.0", tag: "v3.0.2", want: true},
		{baseRef: "release/v3.0", tag: "v3.1.0-0", want: false},
		{baseRef: "release/v3.0", tag: "v2.0.0", want: false},
		{baseRef: "release/v3", tag: "v3.1.0-0", want: true},
		{baseRef: "release/v3", tag: "v4.0.0-0", want: false},
		{baseRef: "feature/release/v3.0", tag: "v3.1.0-0", want: true},
	} {
		require.Equal(t, tc.want, onReleaseLine(tc.baseRef, semver.MustParse(tc.tag)), "%s on %q", tc.tag, tc.baseRef)
	}
}

const retypeMessage = `Field "2" with name "amount" on message "Inner" changed type from "int64" to "string".`

// A listed exception lets its gated break pass only behind a stop that the
// operator lists and that the next build of the branch crosses.
func TestCheckerAppliesWireExceptions(t *testing.T) {
	t.Parallel()

	retyped := strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1)
	exception := func(stop string) []wireException {
		return []wireException{{Declaration: "api.Inner", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: retypeMessage, Stop: stop}}
	}
	// branchPointMarker tags the next line's marker on the latest release,
	// as on a release branch cut without commits of its own.
	branchPointMarker := func(t *testing.T, repo string) {
		t.Helper()
		runGit(t, repo, "tag", "v3.1.0-0")
	}
	// taggedChange puts a later tag on a commit of the checked change.
	taggedChange := func(t *testing.T, repo string) {
		t.Helper()
		writeFixture(t, repo, "CHANGE", "change\n")
		commitFixture(t, repo, "change")
		runGit(t, repo, "tag", "v3.0.0-rc.9")
	}
	// releasedElsewhere tags a release on a branch this one does not contain.
	releasedElsewhere := func(t *testing.T, repo string) {
		t.Helper()
		runGit(t, repo, "tag", "v3.1.0-0")
		runGit(t, repo, "checkout", "--quiet", "-b", "release")
		writeFixture(t, repo, "RELEASE", "v3.0.0\n")
		commitFixture(t, repo, "release")
		runGit(t, repo, "tag", "v3.0.0")
		runGit(t, repo, "checkout", "--quiet", "-")
	}

	for _, tc := range []struct {
		name       string
		proto      string
		exceptions []wireException
		stops      []string
		baseRef    string
		base       string
		arrange    func(t *testing.T, repo string)
		wantGated  int
		excepted   int
		errorSub   string
	}{
		{name: "listed break behind the next release", proto: retyped, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.4", "v3.0.0-rc.5"}, excepted: 1},
		{
			name: "unlisted break", proto: retyped, stops: []string{"v3.0.0-rc.5"},
			exceptions: []wireException{{Declaration: "api.Inner", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: strings.Replace(retypeMessage, `"amount"`, `"count"`, 1), Stop: "v3.0.0-rc.5"}},
			wantGated:  1, errorSub: "1 wire-incompatible change(s) reach a gated service",
		},
		{
			name: "same text on another declaration", proto: retyped, stops: []string{"v3.0.0-rc.5"},
			exceptions: []wireException{{Declaration: "api.GetResponse", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: retypeMessage, Stop: "v3.0.0-rc.5"}},
			wantGated:  1, errorSub: "1 wire-incompatible change(s) reach a gated service",
		},
		{name: "stop missing from the operator", proto: retyped, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.4"}, excepted: 1, errorSub: "relies on the stopped upgrade v3.0.0-rc.5, which mandatoryStopVersions"},
		{name: "stop already released", proto: retyped, exceptions: exception("v3.0.0-rc.4"), stops: []string{"v3.0.0-rc.4"}, excepted: 1, errorSub: "stop v3.0.0-rc.4 does not separate the latest release from the next build of the target branch: use v3.0.0-rc.5"},
		{name: "stop at a later rc", proto: retyped, exceptions: exception("v3.0.0-rc.6"), stops: []string{"v3.0.0-rc.6"}, excepted: 1, errorSub: "stop v3.0.0-rc.6 does not separate"},
		// rc.5 can ship before the final release and would roll.
		{name: "stop at the final release", proto: retyped, exceptions: exception("v3.0.0"), stops: []string{"v3.0.0"}, excepted: 1, errorSub: "stop v3.0.0 does not separate"},
		{name: "merged exception keeps its stop", proto: fixtureProto, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.5"}},
		{name: "merged exception lost its stop", proto: fixtureProto, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.4"}, errorSub: "relies on the stopped upgrade v3.0.0-rc.5"},
		{
			name: "merged exception needs no tag", proto: fixtureProto, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.5"},
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "--delete", "v3.0.0-rc.4")
			},
		},
		{name: "marker moves the default branch to the new line", proto: retyped, exceptions: exception("v3.1.0-0"), stops: []string{"v3.1.0-0"}, arrange: branchPointMarker, excepted: 1},
		{name: "release branch stays on its line", proto: retyped, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.5"}, baseRef: "release/v3.0", arrange: branchPointMarker, excepted: 1},
		{
			name: "release branch rejects the next line's stop", proto: retyped, exceptions: exception("v3.1.0-0"), stops: []string{"v3.1.0-0"},
			baseRef: "refs/heads/release/v3.0", arrange: branchPointMarker, excepted: 1,
			errorSub: "stop v3.1.0-0 does not separate the latest release from the next build of the target branch: use v3.0.0-rc.5",
		},
		// A tag that only the checked change carries is not a release of the
		// target branch.
		{name: "tag on the change does not move the floor", proto: retyped, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.5"}, base: "HEAD~1", arrange: taggedChange, excepted: 1},
		{
			name: "stop after a tag on the change", proto: retyped, exceptions: exception("v3.0.0-rc.10"), stops: []string{"v3.0.0-rc.10"},
			base: "HEAD~1", arrange: taggedChange, excepted: 1,
			errorSub: "stop v3.0.0-rc.10 does not separate the latest release from the next build of the target branch: use v3.0.0-rc.5",
		},
		{name: "stop above a release tagged elsewhere", proto: retyped, exceptions: exception("v3.1.0-0"), stops: []string{"v3.1.0-0"}, arrange: releasedElsewhere, excepted: 1},
		{name: "stop below a release tagged elsewhere", proto: retyped, exceptions: exception("v3.0.0-rc.5"), stops: []string{"v3.0.0-rc.5"}, arrange: releasedElsewhere, excepted: 1, errorSub: "stop v3.0.0-rc.5 does not separate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := newFixtureRepo(t)
			runGit(t, repo, "tag", "v3.0.0-rc.4")
			if tc.arrange != nil {
				tc.arrange(t, repo)
			}
			writeFixture(t, repo, "misc/proto/api.proto", tc.proto)
			writeFixture(t, repo, stopFile, stopSource(tc.stops))
			writeFixture(t, repo, exceptionsFile, exceptionsSource(t, tc.exceptions))
			base := cmp.Or(tc.base, "HEAD")

			res, err := checker{
				repoRoot: repo, base: base, baseRef: tc.baseRef, buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t),
			}.run()
			require.NoError(t, err)
			require.Len(t, res.clientFindings, tc.wantGated, "%v", res.clientFindings)
			require.Equal(t, tc.excepted, res.excepted)

			err = res.verdict()
			if tc.errorSub == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.errorSub)
		})
	}
}

// An exception that the base lists stays listed unchanged: once the break is
// in the base, buf no longer reports it, so nothing else carries its stop.
func TestCheckerKeepsMergedExceptions(t *testing.T) {
	t.Parallel()

	merged := wireException{Declaration: "api.Inner", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: retypeMessage, Stop: "v3.0.0-rc.5"}
	other := wireException{Declaration: "api.GetResponse", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: "another", Stop: "v3.0.0-rc.5"}
	retargeted := merged
	retargeted.Stop = "v3.0.0-rc.6"

	for _, tc := range []struct {
		name       string
		exceptions []wireException
		stops      []string
		noFile     bool
		errorSub   string
	}{
		{name: "kept", exceptions: []wireException{merged}, stops: []string{"v3.0.0-rc.5"}},
		{name: "kept next to a new one", exceptions: []wireException{other, merged}, stops: []string{"v3.0.0-rc.5"}},
		{name: "removed with its stop", errorSub: "the wire exception for api.Inner (stop v3.0.0-rc.5) was removed or changed"},
		{name: "file removed with its stop", noFile: true, errorSub: "the wire exception for api.Inner (stop v3.0.0-rc.5) was removed or changed"},
		{name: "retargeted to a listed stop", exceptions: []wireException{retargeted}, stops: []string{"v3.0.0-rc.6"}, errorSub: "the wire exception for api.Inner (stop v3.0.0-rc.5) was removed or changed"},
		{name: "replaced by another", exceptions: []wireException{other}, stops: []string{"v3.0.0-rc.5"}, errorSub: "the wire exception for api.Inner (stop v3.0.0-rc.5) was removed or changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := newFixtureRepo(t)
			writeFixture(t, repo, stopFile, stopSource([]string{"v3.0.0-rc.5"}))
			writeFixture(t, repo, exceptionsFile, exceptionsSource(t, []wireException{merged}))
			commitFixture(t, repo, "merged exception")

			writeFixture(t, repo, stopFile, stopSource(tc.stops))
			if tc.noFile {
				require.NoError(t, os.Remove(filepath.Join(repo, filepath.FromSlash(exceptionsFile))))
			} else {
				writeFixture(t, repo, exceptionsFile, exceptionsSource(t, tc.exceptions))
			}

			res, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
			require.NoError(t, err)

			err = res.verdict()
			if tc.errorSub == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.errorSub)
		})
	}
}

func TestParseExceptions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		source   string
		want     int
		errorSub string
	}{
		{name: "empty list", source: `{"exceptions": []}`},
		{name: "entry", source: `{"exceptions": [{"declaration": "a.B", "rule": "R", "message": "m", "stop": "v3.0.0-rc.5"}]}`, want: 1},
		{name: "unknown field", source: `{"exceptions": [], "note": "x"}`, errorSub: "unknown field"},
		{name: "missing message", source: `{"exceptions": [{"declaration": "a.B", "rule": "R", "stop": "v3.0.0-rc.5"}]}`, errorSub: "exception 0 needs a declaration, a rule and a message"},
		{name: "missing stop", source: `{"exceptions": [{"declaration": "a.B", "rule": "R", "message": "m"}]}`, errorSub: `exception 0: stop "" is not a canonical version`},
		{name: "not json", source: `exceptions`, errorSub: "invalid character"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseExceptions([]byte(tc.source))
			if tc.errorSub != "" {
				require.ErrorContains(t, err, tc.errorSub)

				return
			}
			require.NoError(t, err)
			require.Len(t, got, tc.want)
		})
	}
}

// The committed list must stay readable by the gate.
func TestParseExceptionsFromRepository(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join(repoRoot(t), exceptionsFile))
	require.NoError(t, err)

	_, err = parseExceptions(source)
	require.NoError(t, err)
}

func TestCheckerFailsClosedOnExceptionStops(t *testing.T) {
	t.Parallel()

	retyped := strings.Replace(fixtureProto, "int64 amount = 2;", "string amount = 2;", 1)

	for _, tc := range []struct {
		name     string
		stop     string
		arrange  func(t *testing.T, repo string)
		errorSub string
	}{
		{name: "no reachable tag", stop: "v3.0.0-rc.5", arrange: func(*testing.T, string) {}, errorSub: "no version tag of the target branch's line is reachable from the base commit"},
		{
			name: "non-canonical stop", stop: "3.0.0-rc.5",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				runGit(t, repo, "tag", "v3.0.0-rc.4")
			},
			errorSub: `exception 0: stop "3.0.0-rc.5" is not a canonical version`,
		},
		{
			name: "unreadable stop list", stop: "v3.0.0-rc.5",
			arrange: func(t *testing.T, repo string) {
				t.Helper()
				writeFixture(t, repo, stopFile, "package controller\n")
			},
			errorSub: "the mandatoryStopVersions variable was not found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := newFixtureRepo(t)
			writeFixture(t, repo, "misc/proto/api.proto", retyped)
			writeFixture(t, repo, stopFile, stopSource([]string{tc.stop}))
			writeFixture(t, repo, exceptionsFile, exceptionsSource(t, []wireException{
				{Declaration: "api.Inner", Rule: "FIELD_WIRE_COMPATIBLE_TYPE", Message: retypeMessage, Stop: tc.stop},
			}))
			tc.arrange(t, repo)

			_, err := checker{repoRoot: repo, base: "HEAD", buf: "buf", services: []string{"api.Api"}, pluginRoot: repoRoot(t)}.run()
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

func commitFixture(t *testing.T, repo, message string) {
	t.Helper()

	runGit(t, repo, "add", ".")
	runGit(t, repo, "-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", message)
}

func exceptionsSource(t *testing.T, exceptions []wireException) string {
	t.Helper()

	source, err := json.Marshal(map[string][]wireException{"exceptions": exceptions})
	require.NoError(t, err)

	return string(source)
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
