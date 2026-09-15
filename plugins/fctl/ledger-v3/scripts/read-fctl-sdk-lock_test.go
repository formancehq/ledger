package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunEmitsTheValidatedLockAsTabSeparatedFields(t *testing.T) {
	t.Parallel()
	lockPath := filepath.Join(t.TempDir(), "fctl-sdk.lock.json")
	input := "{\"schemaVersion\":1,\"modulePath\":\"m\",\"repository\":\"r\",\"commit\":\"c\",\"sdkPath\":\"pkg/plugin\",\"sdkNarHash\":\"h\",\"witPath\":\"wit/plugin.wit\",\"witSha256\":\"w\"}"
	if err := os.WriteFile(lockPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"read-fctl-sdk-lock", lockPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("run exit = %d, stderr = %s", code, stderr.String())
	}
	if got, want := stdout.String(), "m\tr\tc\tpkg/plugin\th\twit/plugin.wit\tw\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

func TestRunFailsClosedWithActionableDiagnostics(t *testing.T) {
	t.Parallel()
	invalidLock := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(invalidLock, []byte("{\"schemaVersion\":2}"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"usage", []string{"read-fctl-sdk-lock"}, "usage: read-fctl-sdk-lock LOCK_FILE"},
		{"open", []string{"read-fctl-sdk-lock", filepath.Join(t.TempDir(), "missing.json")}, "open fctl SDK lock:"},
		{"decode", []string{"read-fctl-sdk-lock", invalidLock}, "decode fctl SDK lock:"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if code := run(test.args, &stdout, &stderr); code != 1 {
				t.Fatalf("run exit = %d, want 1", code)
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("stdout = %q, stderr = %q; want %q", stdout.String(), stderr.String(), test.want)
			}
		})
	}
}

func TestDecodeLockAcceptsTheCompleteContract(t *testing.T) {
	t.Parallel()
	input := "{\"schemaVersion\":1,\"modulePath\":\"m\",\"repository\":\"r\",\"commit\":\"c\",\"sdkPath\":\"pkg/plugin\",\"sdkNarHash\":\"h\",\"witPath\":\"wit/plugin.wit\",\"witSha256\":\"w\"}"
	lock, err := decodeLock(strings.NewReader(input))
	if err != nil {
		t.Fatalf("decodeLock: %v", err)
	}
	if lock.ModulePath != "m" || lock.SDKPath != "pkg/plugin" || lock.WITPath != "wit/plugin.wit" {
		t.Fatalf("decoded lock = %#v", lock)
	}
}

func TestDecodeLockRejectsTrailingJSON(t *testing.T) {
	input := `{"schemaVersion":1,"modulePath":"m","repository":"r","commit":"c","sdkPath":"pkg/plugin","sdkNarHash":"h","witPath":"wit/plugin.wit","witSha256":"w"} {}`
	if _, err := decodeLock(strings.NewReader(input)); err == nil {
		t.Fatal("decodeLock accepted a second JSON value")
	}
}

func TestDecodeLockRejectsUnknownMissingAndUnsafeFields(t *testing.T) {
	t.Parallel()
	tests := []string{
		"{\"schemaVersion\":1,\"unknown\":true}",
		"{\"schemaVersion\":1,\"modulePath\":\"m\",\"repository\":\"r\",\"commit\":\"c\",\"sdkPath\":\"pkg/plugin\",\"sdkNarHash\":\"h\",\"witPath\":\"wit/plugin.wit\",\"witSha256\":\"\"}",
		"{\"schemaVersion\":1,\"modulePath\":\"m\\nunsafe\",\"repository\":\"r\",\"commit\":\"c\",\"sdkPath\":\"pkg/plugin\",\"sdkNarHash\":\"h\",\"witPath\":\"wit/plugin.wit\",\"witSha256\":\"w\"}",
	}
	for _, input := range tests {
		if _, err := decodeLock(strings.NewReader(input)); err == nil {
			t.Fatalf("decodeLock accepted %s", input)
		}
	}
}

func TestValidateRelativePathRejectsNonPortableForms(t *testing.T) {
	for _, path := range []string{
		`../pkg/plugin`,
		`pkg/../plugin`,
		`/pkg/plugin`,
		`C:\\pkg\\plugin`,
		`C:/pkg/plugin`,
		`\\\\server\\share\\plugin`,
		`pkg\\plugin`,
	} {
		t.Run(path, func(t *testing.T) {
			if err := validateRelativePath("sdkPath", path); err == nil {
				t.Fatalf("accepted non-portable path %q", path)
			}
		})
	}
}

func TestValidateRelativePathAcceptsCanonicalSlashPath(t *testing.T) {
	if err := validateRelativePath("sdkPath", "pkg/plugin"); err != nil {
		t.Fatalf("rejected canonical relative path: %v", err)
	}
}
