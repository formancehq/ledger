package sqlstorage

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/go-libs/logging"
	"go.uber.org/fx"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

const (
	KindPostgres = "postgres"
)

// TODO: Replace by logging
func newLogger(debug bool) logger.Interface {
	out := io.Discard
	if debug {
		out = os.Stdout
	}
	return logger.New(
		log.New(out, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Info, // Log level
			IgnoreRecordNotFoundError: false,       // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,       // Disable color
		},
	)
}

func OpenPostgresDatabase(uri string) gorm.Dialector {
	return postgres.Open(uri)
}

func LoadGorm(d gorm.Dialector, debug bool) (*gorm.DB, error) {
	db, err := gorm.Open(d, &gorm.Config{
		Logger: newLogger(debug),
	})
	if err != nil {
		return nil, err
	}
	if err := db.Use(tracing.NewPlugin()); err != nil {
		return nil, err
	}
	return db, nil
}

func MigrateTables(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).AutoMigrate(
		&auth.Client{},
		&auth.AuthRequest{},
		&auth.AccessToken{},
		&auth.AuthRequest{},
		&auth.RefreshToken{},
		&auth.User{},
		&auth.Scope{},
	)
}

var drivers = map[string]func(string) gorm.Dialector{}

func registerDriverConstructor(kind string, constructor func(string) gorm.Dialector) {
	drivers[kind] = constructor
}

func init() {
	registerDriverConstructor(KindPostgres, OpenPostgresDatabase)
}

func gormModule(kind, uri string, debug bool) fx.Option {
	return fx.Options(
		fx.Provide(func(d gorm.Dialector) (*gorm.DB, error) {
			return LoadGorm(d, debug)
		}),
		fx.Invoke(func(lc fx.Lifecycle, db *gorm.DB) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.Info("Migrate tables")
					return MigrateTables(ctx, db)
				},
			})
		}),
		fx.Provide(func() gorm.Dialector {
			return drivers[kind](uri)
		}),
	)
}
