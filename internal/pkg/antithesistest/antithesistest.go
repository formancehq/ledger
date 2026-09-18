// Package antithesistest reads back what an Antithesis assertion actually
// emitted, for tests that assert on emission rather than on behavior.
//
// The SDK resolves ANTITHESIS_SDK_LOCAL_OUTPUT during package initialization,
// before any test can call t.Setenv, so a test cannot observe its own
// assertions: the only way to capture them is to re-exec the test binary with
// the variable already set. Four packages need that and had grown four copies
// of it, differing in ways that were accidents rather than decisions — one
// resolved the binary through os.Args[0] instead of os.Executable(), only one
// escaped the test-name pattern, only one handled subtest paths. A matcher
// that quietly stops matching is indistinguishable from a property that was
// never emitted, so the drift was worth removing.
//
// The helper takes no *testing.T: a failure belongs to the caller's line, and
// no other non-test package here imports testing. Callers assert on the
// returned verdict.
package antithesistest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// Armed reports whether the SDK in this build actually emits. It is a
// constant, so a test can be excluded from an unarmed build at compile time,
// and it is defined here so that every such test agrees on what armed means
// and skips with the same words.
//
// A test needs this when its subject is what an assertion reported rather than
// what the code did: the no-op SDK writes no local output, so there is nothing
// to read back and the test can only skip. Tests that assert on behavior run
// either way and must not consult it.
const Armed = assert.Enabled

// UnarmedSkip is the reason to pass to Skip when Armed is false.
const UnarmedSkip = "requires -tags enable_antithesis_sdk: the no-op SDK writes no local output"

// executedNamedTest reports whether the child actually ran the named test to
// completion. `go test` exits 0 when -test.run matches nothing, and a skip is
// also a pass, so exit status alone cannot distinguish "the property was not
// emitted" from "the test never ran" — and the second silently satisfies every
// expectation of no emission, which is the drift this package exists to catch.
// The -test.v pass marker is the positive proof; subtests print theirs
// indented, hence the contains rather than a prefix match.
func executedNamedTest(logs []byte, run string) bool {
	return bytes.Contains(logs, []byte("--- PASS: "+run+" "))
}

// Emitted runs one test in a fresh copy of the calling test binary and reports
// whether it emitted property with that condition.
//
// run names the test to execute and may be a subtest path ("TestOuter/case"),
// which is anchored segment by segment so a prefix cannot match a sibling.
// dir must be writable and exclusive to this call — t.TempDir() is the
// expected source. A non-zero exit from the child carries its combined output
// in the returned error, since that output is the only account of why.
func Emitted(ctx context.Context, dir, run, property string, condition bool) (found bool, err error) {
	executable, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("resolving the test binary: %w", err)
	}

	output := filepath.Join(dir, "assertions.jsonl")
	pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(run), "/", "$/^") + "$"
	cmd := exec.CommandContext(ctx, executable, "-test.run="+pattern, "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "ANTITHESIS_SDK_LOCAL_OUTPUT="+output)

	logs, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("running %s: %w\n%s", run, err, logs)
	}

	if !executedNamedTest(logs, run) {
		return false, fmt.Errorf(
			"%s did not run: `go test` exits 0 when -test.run matches nothing and when the test skips, "+
				"so a renamed or skipped test would silently satisfy an expectation of no emission\n%s",
			run, logs,
		)
	}

	file, err := os.Open(output)
	if err != nil {
		return false, fmt.Errorf("opening the assertion output of %s: %w", run, err)
	}

	defer func() {
		// This file is the evidence the verdict is read from, so a failed
		// close is reported rather than dropped — but never in place of a
		// failure the read itself already found.
		if closeErr := file.Close(); closeErr != nil && err == nil {
			found, err = false, fmt.Errorf("closing the assertion output of %s: %w", run, closeErr)
		}
	}()

	scanner := bufio.NewScanner(file)
	// One JSONL record carries a whole details map, which can exceed the
	// scanner's default 64 KiB token. That would end the scan with
	// "token too long" and report a read error where the caller expects a
	// verdict, so the limit is raised to something a details map cannot reach.
	scanner.Buffer(nil, 1<<20)

	for scanner.Scan() {
		var event struct {
			Assertion *struct {
				Message   string `json:"message"`
				Hit       bool   `json:"hit"`
				Condition bool   `json:"condition"`
			} `json:"antithesis_assert"`
		}

		if err = json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return false, fmt.Errorf("decoding the assertion output of %s: %w", run, err)
		}

		if a := event.Assertion; a != nil && a.Hit && a.Message == property && a.Condition == condition {
			found = true
		}
	}

	if err = scanner.Err(); err != nil {
		return false, fmt.Errorf("reading the assertion output of %s: %w", run, err)
	}

	return found, nil
}
