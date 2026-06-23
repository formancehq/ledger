package cmdutil

import "fmt"

// IntegrityResult maps the number of integrity errors found by a check onto a
// command result. Any non-zero count is a hard failure so the CLI exits
// non-zero and callers chaining commands (e.g. `restore validate &&
// restore finalize`, or `store bootstrap` proceeding to finalize) stop before
// acting on a corrupt store or backup.
//
// subject names what was checked and is used to build the error message, e.g.
// IntegrityResult("backup validation", 3) yields
// "backup validation failed: 3 integrity error(s)".
func IntegrityResult(subject string, errorCount int) error {
	if errorCount > 0 {
		return fmt.Errorf("%s failed: %d integrity error(s)", subject, errorCount)
	}

	return nil
}
