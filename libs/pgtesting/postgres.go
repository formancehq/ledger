package pgtesting

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type TestingT interface {
	require.TestingT
	Cleanup(func())
}

type pgDatabase struct {
	url string
}

func (s *pgDatabase) ConnString() string {
	return s.url
}

type pgServer struct {
	destroy func() error
	lock    sync.Mutex
	db      *bun.DB
	port    string
	config  config
}

func (s *pgServer) GetPort() int {
	v, err := strconv.ParseInt(s.port, 10, 64)
	if err != nil {
		panic(err)
	}
	return int(v)
}

func (s *pgServer) GetHost() string {
	return "localhost"
}

func (s *pgServer) GetUsername() string {
	return s.config.initialUsername
}

func (s *pgServer) GetPassword() string {
	return s.config.initialUserPassword
}

func (s *pgServer) GetDSN() string {
	return s.GetDatabaseDSN(s.config.initialDatabaseName)
}

func (s *pgServer) GetDatabaseDSN(databaseName string) string {
	return fmt.Sprintf("postgresql://%s:%s@localhost:%s/%s?sslmode=disable", s.config.initialUsername,
		s.config.initialUserPassword, s.port, databaseName)
}

func (s *pgServer) NewDatabase(t TestingT) *pgDatabase {
	s.lock.Lock()
	defer s.lock.Unlock()

	databaseName := uuid.NewString()
	_, err := s.db.ExecContext(context.Background(), fmt.Sprintf(`CREATE DATABASE "%s"`, databaseName))
	require.NoError(t, err)

	if os.Getenv("NO_CLEANUP") != "true" {
		t.Cleanup(func() {
			s.lock.Lock()
			defer s.lock.Unlock()

			_, _ = s.db.ExecContext(context.Background(), fmt.Sprintf(`DROP DATABASE "%s"`, databaseName))
		})
	}

	return &pgDatabase{
		url: s.GetDatabaseDSN(databaseName),
	}
}

func (s *pgServer) Close() error {
	if s.db == nil {
		return nil
	}
	if os.Getenv("NO_CLEANUP") == "true" {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	if err := s.destroy(); err != nil {
		return err
	}
	return nil
}

var srv *pgServer

func Server() *pgServer {
	return srv
}

func NewPostgresDatabase(t TestingT) *pgDatabase {
	return srv.NewDatabase(t)
}

func DestroyPostgresServer() error {
	return srv.Close()
}

type config struct {
	initialDatabaseName string
	initialUserPassword string
	initialUsername     string
	statusCheckInterval time.Duration
	maximumWaitingTime  time.Duration
	context             context.Context
	hostConfigOptions   []func(hostConfig *docker.HostConfig)
}

func (c config) validate() error {
	if c.statusCheckInterval == 0 {
		return errors.New("status check interval must be greater than 0")
	}
	if c.initialUsername == "" {
		return errors.New("initial username must be defined")
	}
	if c.initialUserPassword == "" {
		return errors.New("initial user password must be defined")
	}
	if c.initialDatabaseName == "" {
		return errors.New("initial database name must be defined")
	}
	return nil
}

type option func(opts *config)

func WithInitialDatabaseName(name string) option {
	return func(opts *config) {
		opts.initialDatabaseName = name
	}
}

func WithInitialUser(username, pwd string) option {
	return func(opts *config) {
		opts.initialUserPassword = pwd
		opts.initialUsername = username
	}
}

func WithStatusCheckInterval(d time.Duration) option {
	return func(opts *config) {
		opts.statusCheckInterval = d
	}
}

func WithMaximumWaitingTime(d time.Duration) option {
	return func(opts *config) {
		opts.maximumWaitingTime = d
	}
}

func WithContext(ctx context.Context) option {
	return func(opts *config) {
		opts.context = ctx
	}
}

func WithDockerHostConfigOption(opt func(hostConfig *docker.HostConfig)) option {
	return func(opts *config) {
		opts.hostConfigOptions = append(opts.hostConfigOptions, opt)
	}
}

var defaultOptions = []option{
	WithStatusCheckInterval(200 * time.Millisecond),
	WithInitialUser("root", "root"),
	WithMaximumWaitingTime(5 * time.Second),
	WithInitialDatabaseName("formance"),
	WithContext(context.Background()),
}

func CreatePostgresServer(opts ...option) error {
	cfg := config{}
	for _, opt := range append(defaultOptions, opts...) {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return errors.Wrap(err, "validating config")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return errors.Wrap(err, "unable to start docker containers pool")
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		Env: []string{
			fmt.Sprintf("POSTGRES_USER=%s", cfg.initialUsername),
			fmt.Sprintf("POSTGRES_PASSWORD=%s", cfg.initialUserPassword),
			fmt.Sprintf("POSTGRES_DB=%s", cfg.initialDatabaseName),
		},
		Entrypoint: nil,
		Cmd:        []string{"-c", "superuser-reserved-connections=0"},
	}, cfg.hostConfigOptions...)
	if err != nil {
		return errors.Wrap(err, "unable to start postgres server container")
	}

	srv = &pgServer{
		port: resource.GetPort("5432/tcp"),
		destroy: func() error {
			return pool.Purge(resource)
		},
		config: cfg,
	}

	try := time.Duration(0)
	sqldb, err := sql.Open("postgres", srv.GetDatabaseDSN(cfg.initialDatabaseName))
	if err != nil {
		return err
	}
	srv.db = bun.NewDB(sqldb, pgdialect.New())
	for try*cfg.statusCheckInterval < cfg.maximumWaitingTime {
		err := srv.db.Ping()
		if err != nil {
			try++
			select {
			case <-cfg.context.Done():
				return cfg.context.Err()
			case <-time.After(cfg.statusCheckInterval):
			}
			continue
		}

		return nil
	}

	return errors.New("timeout waiting for server ready")
}
