package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func readStrictJSON(path string, destination any) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a non-symlink regular file", path)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if err := decodeStrictJSON(content, destination); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}

	return content, nil
}

func decodeStrictJSON(content []byte, destination any) error {
	if err := rejectDuplicateObjectKeys(content); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}

	return nil
}

func rejectDuplicateObjectKeys(content []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			keys := map[string]struct{}{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("object key is not a string")
				}
				if _, exists := keys[key]; exists {
					return fmt.Errorf("duplicate object key %q", key)
				}
				keys[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()

			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()

			return err
		default:
			return fmt.Errorf("unexpected delimiter %q", delimiter)
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}

		return err
	}

	return nil
}

func loadCampaign(path string) (*Campaign, error) {
	var campaign Campaign
	if _, err := readStrictJSON(path, &campaign); err != nil {
		return nil, err
	}
	if err := campaign.validate(); err != nil {
		return nil, fmt.Errorf("invalid campaign state: %w", err)
	}

	return &campaign, nil
}

func writeCampaign(path string, campaign *Campaign) error {
	if err := campaign.validate(); err != nil {
		return fmt.Errorf("refusing malformed campaign state: %w", err)
	}
	content, err := json.MarshalIndent(campaign, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal campaign: %w", err)
	}
	content = append(content, '\n')

	return atomicWrite(path, content)
}

func atomicWrite(path string, content []byte) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create campaign directory: %w", err)
	}
	parent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return fmt.Errorf("resolve campaign directory: %w", err)
	}
	base := filepath.Base(path)
	if !filepath.IsLocal(base) || base == "." {
		return errors.New("campaign filename must be local to its destination directory")
	}
	if info, err := os.Lstat(filepath.Join(parent, base)); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("campaign destination must be a non-symlink regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect campaign destination: %w", err)
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return fmt.Errorf("anchor campaign directory: %w", err)
	}
	defer func() {
		_ = root.Close() // Best effort after the result has already been determined.
	}()
	temporary, temporaryName, err := createTemporary(root)
	if err != nil {
		return fmt.Errorf("create campaign temporary file: %w", err)
	}
	cleanup := func() {
		_ = temporary.Close()          // Best effort cleanup.
		_ = root.Remove(temporaryName) // Best effort cleanup.
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()

		return fmt.Errorf("set campaign file mode: %w", err)
	}
	if _, err := temporary.Write(content); err != nil {
		cleanup()

		return fmt.Errorf("write campaign: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()

		return fmt.Errorf("sync campaign: %w", err)
	}
	if err := temporary.Close(); err != nil {
		_ = root.Remove(temporaryName) // Best effort cleanup.

		return fmt.Errorf("close campaign: %w", err)
	}
	if err := root.Rename(temporaryName, base); err != nil {
		_ = root.Remove(temporaryName) // Best effort cleanup.

		return fmt.Errorf("publish campaign: %w", err)
	}
	directory, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("open campaign directory for sync: %w", err)
	}
	defer func() {
		_ = directory.Close() // Best effort after the result has already been determined.
	}()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync campaign directory: %w", err)
	}

	return nil
}

func createTemporary(root *os.Root) (*os.File, string, error) {
	for range 100 {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return nil, "", fmt.Errorf("generate temporary filename: %w", err)
		}
		name := ".ai-campaign-" + hex.EncodeToString(random) + ".tmp"
		file, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}

	return nil, "", errors.New("could not allocate a unique temporary filename")
}

func repositoryRoot() (string, error) {
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}

	return filepath.EvalSymlinks(strings.TrimSpace(string(output)))
}

func validateCampaignDestination(repoRoot, path string, createParent bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve output path: %w", err)
	}
	parent := filepath.Dir(absolute)
	buildRoot := filepath.Join(repoRoot, "build")
	if pathWithin(absolute, repoRoot) && !pathWithin(absolute, buildRoot) {
		return "", errors.New("repository-local campaign state must be stored under build/")
	}

	probe := parent
	missing := []string{}
	for {
		info, statErr := os.Stat(probe)
		if statErr == nil && info.IsDir() {
			break
		}
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf("inspect output ancestor: %w", statErr)
		}
		next := filepath.Dir(probe)
		if next == probe {
			return "", errors.New("output path has no existing ancestor")
		}
		missing = append([]string{filepath.Base(probe)}, missing...)
		probe = next
	}
	physicalProbe, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return "", fmt.Errorf("resolve output ancestor: %w", err)
	}
	physicalParentCandidate := physicalProbe
	for _, component := range missing {
		physicalParentCandidate = filepath.Join(physicalParentCandidate, component)
	}
	physicalCandidate := filepath.Join(physicalParentCandidate, filepath.Base(absolute))
	if pathWithin(physicalCandidate, repoRoot) && !pathWithin(physicalCandidate, buildRoot) {
		return "", errors.New("repository-local campaign state must be stored under build/")
	}
	if !createParent && len(missing) > 0 {
		return "", errors.New("campaign directory does not exist")
	}

	if createParent {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return "", fmt.Errorf("create output directory: %w", err)
		}
	}
	physicalParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	physical := filepath.Join(physicalParent, filepath.Base(absolute))
	insideRepository := pathWithin(physical, repoRoot)
	if !insideRepository {
		return physical, nil
	}
	if !pathWithin(physical, buildRoot) {
		return "", errors.New("repository-local campaign state must be stored under build/")
	}
	command := exec.Command("git", "-C", repoRoot, "check-ignore", "-q", "--", physical)
	if err := command.Run(); err != nil {
		return "", errors.New("repository-local campaign state must be ignored by Git")
	}

	return physical, nil
}

func pathWithin(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}

	return relative == "." || (filepath.IsLocal(relative) && relative != "..")
}
