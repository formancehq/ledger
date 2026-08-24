package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func publish(sourcePath, destination string) error {
	if destination == "" || destination == "." || filepath.Base(destination) != destination {
		return fmt.Errorf("destination must be a basename: %q", destination)
	}

	info, err := os.Lstat(destination)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("destination must not be a symlink: %s", destination)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("destination must be a regular file: %s", destination)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect destination: %w", err)
	}

	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open validated result: %w", err)
	}
	sourceOpen := true
	defer func() {
		if sourceOpen {
			_ = source.Close() // Best effort cleanup; preserve the original error.
		}
	}()

	temporary, err := os.CreateTemp(".", ".ai-audit-result-*")
	if err != nil {
		return fmt.Errorf("create publication temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath != "" {
			_ = os.Remove(temporaryPath) // Best effort cleanup; preserve the original error.
		}
	}()

	if _, err := io.Copy(temporary, source); err != nil {
		_ = temporary.Close() // Best effort cleanup; preserve the copy error.

		return fmt.Errorf("copy validated result: %w", err)
	}
	if err := source.Close(); err != nil {
		_ = temporary.Close() // Best effort cleanup; preserve the source close error.

		return fmt.Errorf("close validated result: %w", err)
	}
	sourceOpen = false
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close() // Best effort cleanup; preserve the sync error.

		return fmt.Errorf("sync validated result: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close publication temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish validated result: %w", err)
	}
	temporaryPath = ""

	return nil
}

func run(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: auditpublish <validated-result> <destination-basename>")
	}

	return publish(args[0], args[1])
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "auditpublish: %v\n", err)
		os.Exit(1)
	}
}
