package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// The Antithesis SDK is selected three times over, in files that no build step
// compares: the go.mod replace picks the fork for the compiler, the Dockerfiles
// clone that fork to build the instrumentor, and -instrumentor_version tells
// the generated notifier module which SDK version to require.
//
// Drift between them is silent and expensive. An instrumentor built from a
// different commit than the SDK the binary links does not recognise the
// `if assert.Enabled` shape and synthesizes an unreachable coverage edge at
// every guarded site; a stale -instrumentor_version lets a stable upstream
// release outrank the prerelease if the replace is ever dropped, which selects
// the live-by-default SDK and changes what production builds do. Both pass
// every gate. This is the one review finding on this work that was blocking,
// so the pins are compared here rather than by comment.
const (
	sdkModulePath  = "github.com/antithesishq/antithesis-sdk-go"
	rootGoMod      = "go.mod"
	workloadGoMod  = "tests/antithesis/workload/go.mod"
	rootDockerfile = "Dockerfile.antithesis"
	workloadDocker = "tests/antithesis/workload/Dockerfile"
)

var (
	sdkRequire  = regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(sdkModulePath) + ` (v[^\s]+)$`)
	sdkReplace  = regexp.MustCompile(`(?m)^replace ` + regexp.QuoteMeta(sdkModulePath) + ` => (\S+) v0\.0\.0-\d+-([0-9a-f]{12})$`)
	sdkClone    = regexp.MustCompile(`git clone --quiet https://(\S+) /sdk`)
	sdkCheckout = regexp.MustCompile(`git -C /sdk checkout --quiet ([0-9a-f]{40})`)
	// Anchored on the command: the comment above it says the flag's name too.
	sdkInstrumentV = regexp.MustCompile(`antithesis-go-instrumentor -instrumentor_version (\S+)`)
)

// checkSDKPin reports disagreement between the places the SDK is pinned.
func checkSDKPin() ([]finding, error) {
	sources := map[string]string{}

	for _, path := range []string{rootGoMod, workloadGoMod, rootDockerfile, workloadDocker} {
		source, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		sources[path] = string(source)
	}

	return checkSDKPinIn(sources), nil
}

func checkSDKPinIn(sources map[string]string) []finding {
	var findings []finding

	// Each map is value -> the files that state it, so a disagreement names
	// every side rather than an arbitrary one.
	requires := map[string][]string{}
	forks := map[string][]string{}
	shas := map[string][]string{}
	instrumentors := map[string][]string{}

	for _, path := range []string{rootGoMod, workloadGoMod} {
		source := sources[path]

		if m := sdkRequire.FindStringSubmatch(source); m != nil {
			requires[m[1]] = append(requires[m[1]], path)
		} else {
			findings = append(findings, sdkPinFinding(path, "no `"+sdkModulePath+" <version>` requirement found"))
		}

		if m := sdkReplace.FindStringSubmatch(source); m != nil {
			forks[m[1]] = append(forks[m[1]], path)
			shas[m[2]] = append(shas[m[2]], path)
		} else {
			findings = append(findings, sdkPinFinding(path,
				"no `replace "+sdkModulePath+" => <fork> v0.0.0-<date>-<sha>` found; the fork is reachable only through this replace"))
		}
	}

	for _, path := range []string{rootDockerfile, workloadDocker} {
		source := sources[path]

		if m := sdkClone.FindStringSubmatch(source); m != nil {
			forks[strings.TrimSuffix(m[1], ".git")] = append(forks[strings.TrimSuffix(m[1], ".git")], path)
		} else {
			findings = append(findings, sdkPinFinding(path, "no `git clone ... /sdk` of the fork found"))
		}

		if m := sdkCheckout.FindStringSubmatch(source); m != nil {
			shas[m[1][:12]] = append(shas[m[1][:12]], path)
		} else {
			findings = append(findings, sdkPinFinding(path, "no `git -C /sdk checkout <40-hex sha>` found"))
		}

		if m := sdkInstrumentV.FindStringSubmatch(source); m != nil {
			instrumentors[m[1]] = append(instrumentors[m[1]], path)
		} else {
			findings = append(findings, sdkPinFinding(path,
				"no -instrumentor_version found; it defaults to the instrumentor's own SDK_Version, which would outrank the prerelease if the replace were dropped"))
		}
	}

	findings = append(findings, sdkPinAgreement("SDK version", requires, instrumentors)...)
	findings = append(findings, sdkPinAgreement("fork module path", forks, nil)...)
	findings = append(findings, sdkPinAgreement("fork commit", shas, nil)...)

	return findings
}

// sdkPinAgreement reports one finding per value when the same thing is stated
// more than one way across the two groups.
func sdkPinAgreement(subject string, primary, secondary map[string][]string) []finding {
	merged := map[string][]string{}
	for value, paths := range primary {
		merged[value] = append(merged[value], paths...)
	}

	for value, paths := range secondary {
		merged[value] = append(merged[value], paths...)
	}

	if len(merged) < 2 {
		return nil
	}

	values := make([]string, 0, len(merged))
	for value := range merged {
		values = append(values, value)
	}

	sort.Strings(values)

	var stated []string

	for _, value := range values {
		paths := merged[value]
		sort.Strings(paths)
		stated = append(stated, fmt.Sprintf("%s (%s)", value, strings.Join(paths, ", ")))
	}

	message := fmt.Sprintf(
		"the %s is stated %d ways and they must agree: %s",
		subject, len(merged), strings.Join(stated, " vs "),
	)

	// One finding per file that states one of the values: the message names
	// every side, but triage follows the location, and anchoring a Dockerfile
	// drift at go.mod:1 sends the reader to the wrong file.
	var paths []string

	for _, statedIn := range merged {
		paths = append(paths, statedIn...)
	}

	sort.Strings(paths)

	findings := make([]finding, 0, len(paths))
	for _, path := range paths {
		findings = append(findings, sdkPinFinding(path, message))
	}

	return findings
}

func sdkPinFinding(path, message string) finding {
	return finding{path: path, line: 1, column: 1, message: "SDK_PIN_DRIFT: " + message}
}
