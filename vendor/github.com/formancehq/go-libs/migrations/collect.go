package migrations

import (
	"context"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

//go:generate mockgen -source collect.go -destination collect_generated.go -package migrations . MigrationFileSystem
type MigrationFileSystem interface {
	ReadDir(dir string) ([]fs.DirEntry, error)
	ReadFile(filename string) ([]byte, error)
}

func CollectMigrationFiles(fs MigrationFileSystem, rootDir string, transformer func(string) string) ([]Migration, error) {
	entries, err := fs.ReadDir(rootDir)
	if err != nil {
		return nil, errors.Wrap(err, "collecting migration files")
	}

	filenames := make([]string, len(entries))
	for i, entry := range entries {
		filenames[i] = entry.Name()
	}

	slices.SortFunc(filenames, func(a, b string) int {
		fileAVersionAsString := strings.SplitN(a, "-", 2)[0]
		fileAVersion, err := strconv.ParseInt(fileAVersionAsString, 10, 64)
		if err != nil {
			panic(err)
		}

		fileBVersionAsString := strings.SplitN(b, "-", 2)[0]
		fileBVersion, err := strconv.ParseInt(fileBVersionAsString, 10, 64)
		if err != nil {
			panic(err)
		}

		return int(fileAVersion - fileBVersion)
	})

	ret := make([]Migration, len(entries))
	for i, entry := range filenames {
		fileContent, err := fs.ReadFile(filepath.Join(rootDir, entry))
		if err != nil {
			return nil, errors.Wrapf(err, "reading migration file %s", entry)
		}

		ret[i] = Migration{
			Name: entry,
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, transformer(string(fileContent)))
				return err
			},
		}
	}

	return ret, nil
}
