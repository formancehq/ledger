package restoreseed

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ValidateLocation rejects configurations where installing the read index
// could create or replace another store's live database, checkpoint or
// staging directory. Resolve symlinks in existing ancestors even when the
// index directory itself does not exist yet.
func ValidateLocation(dataDir, indexDir string, otherStores ...string) error {
	data, err := resolvedPath(dataDir)
	if err != nil {
		return err
	}
	indexLex, err := filepath.Abs(indexDir)
	if err != nil {
		return err
	}
	dataLex, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	index, err := resolvedPath(indexDir)
	if err != nil {
		return err
	}
	for _, name := range []string{"live", "live.staging", "live.discard", "checkpoints", "query-checkpoints", "incoming-checkpoint", "tmp", "restore-staging", stageDir, "usage", "spool"} {
		// The directory names are reserved even before they exist. Case fold
		// conservatively so aliases on case-insensitive volumes cannot bypass this.
		reserved, err := resolvedPath(filepath.Join(data, name))
		if err != nil {
			return err
		}
		if inside(index, reserved) || inside(indexLex, filepath.Join(dataLex, name)) {
			return fmt.Errorf("read-index directory %s overlaps reserved %s directory", indexDir, name)
		}
	}
	for _, raw := range otherStores {
		if raw == "" {
			continue
		}
		reserved, err := resolvedPath(raw)
		if err != nil {
			return err
		}
		if inside(index, reserved) {
			return fmt.Errorf("read-index directory %s overlaps storage directory %s", indexDir, raw)
		}
	}

	return nil
}

func inside(path, ancestor string) bool {
	rel, err := filepath.Rel(strings.ToLower(ancestor), strings.ToLower(path))

	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

func resolvedPath(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	var suffix []string
	for {
		_, err = os.Lstat(path)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", err
		}
		suffix = append(suffix, filepath.Base(path))
		path = parent
	}
	prefix, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	for _, s := range slices.Backward(suffix) {
		prefix = filepath.Join(prefix, s)
	}

	return prefix, nil
}
