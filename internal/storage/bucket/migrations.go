package bucket

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"
)

//go:embed migrations
var MigrationsFS embed.FS

func GetMigrator(db bun.IDB, name string, options ...migrations.Option) *migrations.Migrator {
	options = append(options, migrations.WithSchema(name))
	migrator := migrations.NewMigrator(db, options...)

	_, transactional := db.(bun.Tx)

	collectOptions := make([]CollectOption, 0)
	if transactional {
		collectOptions = append(collectOptions, WithTemplateVars(map[string]any{
			"Transactional": true,
		}))
	}

	allMigrations, err := CollectMigrations(MigrationsFS, name, collectOptions...)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(allMigrations...)

	return migrator
}

func runMigrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string, options ...migrations.Option) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	migrator := GetMigrator(db, name, options...)

	for {
		err := migrator.UpByOne(ctx)
		if err != nil {
			if errors.Is(err, migrations.ErrAlreadyUpToDate) {
				return nil
			}
			return err
		}
	}
}

type MigrationFileSystem interface {
	ReadDir(dir string) ([]fs.DirEntry, error)
	ReadFile(filename string) ([]byte, error)
}

type notes struct {
	Name string `yaml:"name"`
}

type collectOptions struct {
	templateVars map[string]any
}

type CollectOption func(*collectOptions)

func WithTemplateVars(vars map[string]any) CollectOption {
	return func(o *collectOptions) {
		o.templateVars = vars
	}
}

func CollectMigrations(_fs MigrationFileSystem, dir string, options ...CollectOption) ([]migrations.Migration, error) {
	return WalkMigrations(_fs, func(entry fs.DirEntry) (*migrations.Migration, error) {
		rawNotes, err := _fs.ReadFile(filepath.Join("migrations", entry.Name(), "notes.yaml"))
		if err != nil {
			return nil, fmt.Errorf("failed to read notes.yaml: %w", err)
		}

		notes := &notes{}
		if err := yaml.Unmarshal(rawNotes, notes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal notes.yaml: %w", err)
		}

		co := collectOptions{}
		for _, option := range options {
			option(&co)
		}

		sqlFile, err := TemplateSQLFile(_fs, dir, entry.Name(), "up.sql", co.templateVars)
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

func WalkMigrations[T any](_fs MigrationFileSystem, transformer func(entry fs.DirEntry) (*T, error)) ([]T, error) {
	entries, err := _fs.ReadDir("migrations")
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

func TemplateSQLFile(_fs MigrationFileSystem, schema, migrationDir, file string, vars map[string]any) (string, error) {
	rawSQL, err := _fs.ReadFile(filepath.Join("migrations", migrationDir, file))
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", file, err)
	}

	if vars == nil {
		vars = map[string]any{}
	}
	vars["Schema"] = schema

	buf := bytes.NewBuffer(nil)
	err = template.Must(template.New("migration").
		Parse(string(rawSQL))).
		Execute(buf, vars)
	if err != nil {
		panic(err)
	}

	return buf.String(), nil
}
