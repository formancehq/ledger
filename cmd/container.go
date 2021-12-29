package cmd

import (
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"net/http"
)

func NewContainer(v *viper.Viper, options ...fx.Option) *fx.App {

	if !v.GetBool(debugFlag) {
		options = append(options, fx.NopLogger)
	}

	// Handle OpenTelemetry
	if v.GetBool(otelFlag) {
		options = append(options, opentelemetry.Module(opentelemetry.Config{
			ServiceName: "ledger",
			Version:     Version,
			Exporter:    v.GetString(otelExporterFlag),
			JaegerConfig: func() *opentelemetry.JaegerConfig {
				if v.GetString(otelExporterFlag) != opentelemetry.JaegerExporter {
					return nil
				}
				return &opentelemetry.JaegerConfig{
					Endpoint: v.GetString(otelExporterJaegerEndpointFlag),
					User:     v.GetString(otelExporterJaegerUserFlag),
					Password: v.GetString(otelExporterJaegerPasswordFlag),
				}
			}(),
			OTLPConfig: func() *opentelemetry.OTLPConfig {
				if v.GetString(otelExporterFlag) != opentelemetry.OTLPExporter {
					return nil
				}
				return &opentelemetry.OTLPConfig{
					Mode:     v.GetString(otelExporterOTLPModeFlag),
					Endpoint: v.GetString(otelExporterOTLPEndpointFlag),
					Insecure: v.GetBool(otelExporterOTLPInsecureFlag),
				}
			}(),
			ApiMiddlewareName: "ledger",
		}))
	}

	// Handle api part
	options = append(options, api.Module(api.Config{
		StorageDriver: v.GetString(storageDriverFlag),
		LedgerLister: controllers.LedgerListerFn(func(*http.Request) []string {
			return v.GetStringSlice(ledgersFlag)
		}),
		HttpBasicAuth: v.GetString(serverHttpBasicAuthFlag),
		Version:       Version,
	}))

	// Handle storage driver
	options = append(options, sqlstorage.DriverModule(sqlstorage.ModuleConfig{
		StorageDriver: v.GetString(storageDriverFlag),
		SQLiteConfig: func() *sqlstorage.SQLiteConfig {
			if v.GetString(storageDriverFlag) != sqlstorage.SQLite.String() {
				return nil
			}
			return &sqlstorage.SQLiteConfig{
				Dir:    v.GetString(storageDirFlag),
				DBName: v.GetString(storageSQLiteDBNameFlag),
			}
		}(),
		PostgresConfig: func() *sqlstorage.PostgresConfig {
			if v.GetString(storageDriverFlag) != sqlstorage.PostgreSQL.String() {
				return nil
			}
			return &sqlstorage.PostgresConfig{
				ConnString: v.GetString(storagePostgresConnectionStringFlag),
			}
		}(),
	}))

	// Handle resolver
	options = append(options, ledger.ResolveModule())

	// fx has issues about decorators feature. We will wait until released.
	options = append(options,
		fx.Provide(func(driver storage.Driver) storage.Factory {
			f := storage.NewDefaultFactory(driver)
			if v.GetBool(storageCacheFlag) {
				f = storage.NewCachedStorageFactory(f)
			}
			if v.GetBool(persistConfigFlag) {
				f = storage.NewRememberConfigStorageFactory(f)
			}
			if v.GetBool(otelFlag) {
				f = opentelemetry.NewOpenTelemetryStorageFactory(f)
			}
			return f
		}),
	)

	return fx.New(options...)
}
