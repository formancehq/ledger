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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

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
	cmd := exec.CommandContext(ctx, executable, "-test.run="+pattern, "-test.count=1")
	cmd.Env = append(os.Environ(), "ANTITHESIS_SDK_LOCAL_OUTPUT="+output)

	logs, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("running %s: %w\n%s", run, err, logs)
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
