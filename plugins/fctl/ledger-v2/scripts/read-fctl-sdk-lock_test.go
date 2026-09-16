package main

import (
	"strings"
	"testing"
)

func TestDecodeLockRejectsTrailingJSON(t *testing.T) {
	input := `{"schemaVersion":1,"modulePath":"m","repository":"r","commit":"c","sdkPath":"pkg/plugin","sdkNarHash":"h","witPath":"wit/plugin.wit","witSha256":"w"} {}`
	if _, err := decodeLock(strings.NewReader(input)); err == nil {
		t.Fatal("decodeLock accepted a second JSON value")
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
