package migrations

import (
	"flag"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/ory/dockertest/v3"
	dockerlib "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"github.com/xo/dburl"
	"os"
	"strings"
	"testing"
)

var (
	sourceDatabase      string
	destinationDatabase string
	skipCopy            bool
	skipMigrate         bool
)

func TestMain(m *testing.M) {
	flag.StringVar(&sourceDatabase, "databases.source", "", "Source database")
	flag.StringVar(&destinationDatabase, "databases.destination", "", "Destination database")
	flag.BoolVar(&skipCopy, "skip-copy", false, "Skip copying database")
	flag.BoolVar(&skipMigrate, "skip-migrate", false, "Skip migrating database")
	flag.Parse()

	os.Exit(m.Run())
}

func TestMigrations(t *testing.T) {

	ctx := logging.TestingContext()
	dockerPool := docker.NewPool(t, logging.Testing())

	if destinationDatabase == "" {
		pgServer := pgtesting.CreatePostgresServer(t, dockerPool)
		destinationDatabase = pgServer.GetDSN()
	}

	if !skipCopy {
		if sourceDatabase == "" {
			t.Skip()
		}

		copyDatabase(t, dockerPool, sourceDatabase, destinationDatabase)
		fmt.Println("Database copied")
	}

	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
		DatabaseSourceName: destinationDatabase,
	})
	require.NoError(t, err)

	if skipMigrate {
		return
	}

	// Migrate database
	driver := driver.New(
		db,
		ledger.NewFactory(db),
		bucket.NewDefaultFactory(),
		system.NewStoreFactory(),
		driver.WithParallelBucketMigration(1),
	)
	require.NoError(t, driver.Initialize(ctx))
	require.NoError(t, driver.UpgradeAllBuckets(ctx))
}

func copyDatabase(t *testing.T, dockerPool *docker.Pool, source, destination string) {
	resource := dockerPool.Run(docker.Configuration{
		RunOptions: &dockertest.RunOptions{
			Repository: "postgres",
			Tag:        "15-alpine",
			Entrypoint: []string{"sleep", "infinity"},
		},
		HostConfigOptions: []func(config *dockerlib.HostConfig){
			func(config *dockerlib.HostConfig) {
				config.NetworkMode = "host"
			},
		},
	})

	execArgs := []string{"sh", "-c", fmt.Sprintf(`
		%s | %s
	`,
		preparePGDumpCommand(t, source),
		preparePSQLCommand(t, destination),
	)}

	fmt.Printf("Exec command: %s\n", execArgs)

	_, err := resource.Exec(execArgs, dockertest.ExecOptions{
		StdOut: os.Stdout,
		StdErr: os.Stdout,
	})

	require.NoError(t, err)
}

func preparePGDumpCommand(t *testing.T, dsn string) string {
	parsedSource, err := dburl.Parse(dsn)
	require.NoError(t, err)

	args := make([]string, 0)

	password, ok := parsedSource.User.Password()
	if ok {
		args = append(args, "PGPASSWORD="+password)
	}

	args = append(args,
		"pg_dump",
		"--no-owner", // skip roles
		"-x",         // Skip privileges
		"-h", parsedSource.Hostname(),
		"-p", parsedSource.Port(),
		"-v",
	)

	if username := parsedSource.User.Username(); username != "" {
		args = append(args, "-U", username)
	}

	return strings.Join(append(args, parsedSource.Path[1:]), " ")
}

func preparePSQLCommand(t *testing.T, dsn string) string {
	parsedSource, err := dburl.Parse(dsn)
	require.NoError(t, err)

	args := make([]string, 0)

	password, ok := parsedSource.User.Password()
	if ok {
		args = append(args, "PGPASSWORD="+password)
	}

	args = append(args,
		"psql",
		"--echo-all",
		"-h", parsedSource.Hostname(),
		"-p", parsedSource.Port(),
		parsedSource.Path[1:],
	)

	if username := parsedSource.User.Username(); username != "" {
		args = append(args, "-U", username)
	}

	return strings.Join(args, " ")
}
