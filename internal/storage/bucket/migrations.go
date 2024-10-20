package bucket

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/ghodss/yaml"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"
)

//go:embed migrations
var MigrationsFS embed.FS

func GetMigrator(name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	migrations, err := collectMigrations(name)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(migrations...)

	return migrator
}

func migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(name).Up(ctx, db)
}

type notes struct {
	Name string `yaml:"name"`
}

func collectMigrations(name string) ([]migrations.Migration, error) {
	return WalkMigrations(func(entry fs.DirEntry) (*migrations.Migration, error) {
		rawNotes, err := MigrationsFS.ReadFile(filepath.Join("migrations", entry.Name(), "notes.yaml"))
		if err != nil {
			return nil, fmt.Errorf("failed to read notes.yaml: %w", err)
		}

		notes := &notes{}
		if err := yaml.Unmarshal(rawNotes, notes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal notes.yaml: %w", err)
		}

		sqlFile, err := TemplateSQLFile(name, entry.Name(), "up.sql")
		if err != nil {
			return nil, fmt.Errorf("failed to template sql file: %w", err)
		}

		return &migrations.Migration{
			Name: notes.Name,
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, sqlFile)
				return err
			},
		}, nil
	})
}

func WalkMigrations[T any](transformer func(entry fs.DirEntry) (*T, error)) ([]T, error) {
	entries, err := MigrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		fileAVersionAsString := strings.SplitN(a.Name(), "-", 2)[0]
		fileAVersion, err := strconv.ParseInt(fileAVersionAsString, 10, 64)
		if err != nil {
			panic(err)
		}

		fileBVersionAsString := strings.SplitN(b.Name(), "-", 2)[0]
		fileBVersion, err := strconv.ParseInt(fileBVersionAsString, 10, 64)
		if err != nil {
			panic(err)
		}

		return int(fileAVersion - fileBVersion)
	})

	ret := make([]T, len(entries))
	for i, entry := range entries {
		transformed, err := transformer(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to transform entry: %w", err)
		}
		ret[i] = *transformed
	}

	return ret, nil
}

func TemplateSQLFile(bucket, migrationDir, file string) (string, error) {
	rawSQL, err := MigrationsFS.ReadFile(filepath.Join("migrations", migrationDir, file))
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", file, err)
	}

	buf := bytes.NewBuffer(nil)
	err = template.Must(template.New("migration").
		Parse(string(rawSQL))).
		Execute(buf, map[string]any{
			"Bucket": bucket,
		})
	if err != nil {
		panic(err)
	}

	return buf.String(), nil
}
