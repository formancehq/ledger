package bucket

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
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
var migrationsDir embed.FS

func GetMigrator(name string) *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema(name, true))
	migrations, err := collectMigrations(name)
	if err != nil {
		panic(err)
	}
	migrator.RegisterMigrations(migrations...)

	return migrator
}

func Migrate(ctx context.Context, tracer trace.Tracer, db bun.IDB, name string) error {
	ctx, span := tracer.Start(ctx, "Migrate bucket")
	defer span.End()

	return GetMigrator(name).Up(ctx, db)
}

type Notes struct {
	Name string `yaml:"name"`
}

func collectMigrations(name string) ([]migrations.Migration, error) {
	entries, err := migrationsDir.ReadDir("migrations")
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

	ret := make([]migrations.Migration, len(entries))
	for i, entry := range entries {
		rawNotes, err := migrationsDir.ReadFile(filepath.Join("migrations", entry.Name(), "notes.yaml"))
		if err != nil {
			return nil, fmt.Errorf("failed to read notes.yaml: %w", err)
		}

		notes := &Notes{}
		if err := yaml.Unmarshal(rawNotes, notes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal notes.yaml: %w", err)
		}

		rawSQL, err := migrationsDir.ReadFile(filepath.Join("migrations", entry.Name(), "migration.sql"))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration.sql: %w", err)
		}

		buf := bytes.NewBuffer(nil)
		err = template.Must(template.New("migration").
			Parse(string(rawSQL))).
			Execute(buf, map[string]any{
				"Bucket": name,
			})
		if err != nil {
			return nil, fmt.Errorf("failed to execute template: %w", err)
		}

		ret[i] = migrations.Migration{
			Name: notes.Name,
			Up: func(ctx context.Context, db bun.IDB) error {
				logging.FromContext(ctx).Infof("Applying migration %s", notes.Name)
				_, err := db.ExecContext(ctx, buf.String())
				return err
			},
		}
	}

	return ret, nil
}
