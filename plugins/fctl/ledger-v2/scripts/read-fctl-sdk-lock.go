package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

type sdkLock struct {
	SchemaVersion int    `json:"schemaVersion"`
	ModulePath    string `json:"modulePath"`
	Repository    string `json:"repository"`
	Commit        string `json:"commit"`
	SDKPath       string `json:"sdkPath"`
	SDKNarHash    string `json:"sdkNarHash"`
	WITPath       string `json:"witPath"`
	WITSHA256     string `json:"witSha256"`
}

func main() {
	if len(os.Args) != 2 {
		fatalf("usage: read-fctl-sdk-lock LOCK_FILE")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fatalf("open fctl SDK lock: %v", err)
	}
	defer f.Close()

	lock, err := decodeLock(f)
	if err != nil {
		fatalf("decode fctl SDK lock: %v", err)
	}
	fields := []string{
		lock.ModulePath,
		lock.Repository,
		lock.Commit,
		lock.SDKPath,
		lock.SDKNarHash,
		lock.WITPath,
		lock.WITSHA256,
	}

	fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\n", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6])
}

func decodeLock(r io.Reader) (sdkLock, error) {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var lock sdkLock
	if err := decoder.Decode(&lock); err != nil {
		return sdkLock{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return sdkLock{}, fmt.Errorf("trailing JSON value")
		}
		return sdkLock{}, fmt.Errorf("trailing data: %w", err)
	}
	if lock.SchemaVersion != 1 {
		return sdkLock{}, fmt.Errorf("unsupported schema version: %d", lock.SchemaVersion)
	}
	fields := []string{
		lock.ModulePath,
		lock.Repository,
		lock.Commit,
		lock.SDKPath,
		lock.SDKNarHash,
		lock.WITPath,
		lock.WITSHA256,
	}
	for _, value := range fields {
		if value == "" || strings.ContainsAny(value, "\t\r\n") {
			return sdkLock{}, fmt.Errorf("lock contains an empty or unsafe field")
		}
	}
	if err := validateRelativePath("sdkPath", lock.SDKPath); err != nil {
		return sdkLock{}, err
	}
	if err := validateRelativePath("witPath", lock.WITPath); err != nil {
		return sdkLock{}, err
	}
	return lock, nil
}

func validateRelativePath(name, value string) error {
	if strings.Contains(value, `\`) || strings.HasPrefix(value, "/") || hasWindowsDrivePrefix(value) || path.Clean(value) != value || value == "." || value == ".." || strings.HasPrefix(value, "../") {
		return fmt.Errorf("fctl SDK lock %s must be a canonical portable relative path", name)
	}
	return nil
}

func hasWindowsDrivePrefix(value string) bool {
	if len(value) < 2 || value[1] != ':' {
		return false
	}
	return value[0] >= 'A' && value[0] <= 'Z' || value[0] >= 'a' && value[0] <= 'z'
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
