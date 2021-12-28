package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/numary/machine/script/compiler"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
)

const (
	debugFlag                            = "debug"
	storageDirFlag                       = "storage.dir"
	storageDriverFlag                    = "storage.driver"
	storageSQLiteDBNameFlag              = "storage.sqlite.db_name"
	storagePostgresConnectionStringFlagd = "storage.postgres.conn_string"
	storageCacheFlag                     = "storage.cache"
	persistConfigFlag                    = "persist-config"
	serverHttpBindAddressFlag            = "server.http.bind_address"
	uiHttpBindAddressFlag                = "ui.http.bind_address"
	ledgersFlag                          = "ledgers"
	otelFlag                             = "otel"
	otelExporterFlag                     = "otel-exporter"
	serverHttpBasicAuthFlag              = "server.http.basic_auth"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func NewRootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "numary",
		Short:             "Numary",
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			err := os.MkdirAll(viper.GetString(storageDirFlag), 0700)
			if err != nil {
				return errors.Wrap(err, "creating storage directory")
			}

			if viper.GetBool(debugFlag) {
				logrus.StandardLogger().Level = logrus.DebugLevel
			}
			return nil
		},
	}

	server := &cobra.Command{
		Use: "server",
	}

	version := &cobra.Command{
		Use:   "version",
		Short: "Get version",
		Run:   PrintVersion,
	}

	start := &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			app, err := createContainer(
				WithOption(fx.Invoke(func(h *api.API) error {
					listener, err := net.Listen("tcp", viper.GetString(serverHttpBindAddressFlag))
					if err != nil {
						return err
					}

					go http.Serve(listener, h)
					go func() {
						select {
						case <-cmd.Context().Done():
						}
						err := listener.Close()
						if err != nil {
							panic(err)
						}
					}()

					return nil
				})),
			)
			if err != nil {
				return err
			}
			terminated := make(chan struct{})
			go func() {
				app.Run()
				close(terminated)
			}()
			select {
			case <-cmd.Context().Done():
				return app.Stop(context.Background())
			case <-terminated:
			}

			return nil
		},
	}

	server.AddCommand(start)

	conf := &cobra.Command{
		Use: "config",
	}

	conf.AddCommand(&cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			err := viper.SafeWriteConfig()
			if err != nil {
				logrus.Println(err)
			}
		},
	})

	store := &cobra.Command{
		Use: "storage",
	}

	store.AddCommand(&cobra.Command{
		Use: "init",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := createContainer(
				WithOption(fx.Invoke(func(storageFactory storage.Factory) error {
					s, err := storageFactory.GetStore("default")
					if err != nil {
						return err
					}

					err = s.Initialize(context.Background())
					if err != nil {
						return err
					}
					return nil
				})),
			)
			if err != nil {
				return err
			}
			return nil
		},
	})

	scriptExec := &cobra.Command{
		Use:  "exec [ledger] [script]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			b, err := ioutil.ReadFile(args[1])
			if err != nil {
				logrus.Fatal(err)
			}

			r := regexp.MustCompile(`^\n`)
			s := string(b)
			s = r.ReplaceAllString(s, "")

			b, err = json.Marshal(gin.H{
				"plain": string(s),
			})
			if err != nil {
				logrus.Fatal(err)
			}

			res, err := http.Post(
				fmt.Sprintf(
					"http://%s/%s/script",
					viper.Get(serverHttpBindAddressFlag),
					args[0],
				),
				"application/json",
				bytes.NewReader([]byte(b)),
			)
			if err != nil {
				logrus.Fatal(err)
			}

			b, err = ioutil.ReadAll(res.Body)
			if err != nil {
				logrus.Fatal(err)
			}

			var result struct {
				Err string `json:"err,omitempty"`
				Ok  bool   `json:"ok"`
			}
			err = json.Unmarshal(b, &result)
			if err != nil {
				logrus.Fatal(err)
			}

			if result.Ok {
				fmt.Println("Script ran successfully ✅")
			} else {
				logrus.Fatal(result.Err)
			}
		},
	}

	scriptCheck := &cobra.Command{
		Use:  "check [script]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			b, err := ioutil.ReadFile(args[0])
			if err != nil {
				logrus.Fatal(err)
			}

			_, err = compiler.Compile(string(b))
			if err != nil {
				logrus.Fatal(err)
			} else {
				fmt.Println("Script is correct ✅")
			}
		},
	}

	root.AddCommand(server)
	root.AddCommand(conf)
	root.AddCommand(UICmd)
	root.AddCommand(store)
	root.AddCommand(scriptExec)
	root.AddCommand(scriptCheck)
	root.AddCommand(version)
	root.AddCommand(stickersCmd)

	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	root.PersistentFlags().Bool(debugFlag, false, "Debug mode")
	root.PersistentFlags().String(storageDriverFlag, "sqlite", "Storage driver")
	root.PersistentFlags().String(storageDirFlag, path.Join(home, ".numary/data"), "Storage directory (for sqlite)")
	root.PersistentFlags().String(storageSQLiteDBNameFlag, "numary", "SQLite database name")
	root.PersistentFlags().String(storagePostgresConnectionStringFlagd, "postgresql://localhost/postgres", "Postgre connection string")
	root.PersistentFlags().Bool(storageCacheFlag, true, "Storage cache")
	root.PersistentFlags().Bool(persistConfigFlag, true, "Persist config on disk")
	root.PersistentFlags().String(serverHttpBindAddressFlag, "localhost:3068", "API bind address")
	root.PersistentFlags().String(uiHttpBindAddressFlag, "localhost:3068", "UI bind address")
	root.PersistentFlags().StringSlice(ledgersFlag, []string{"quickstart"}, "Ledgers")
	root.PersistentFlags().Bool(otelFlag, false, "Enable OpenTelemetry support")
	root.PersistentFlags().String(otelExporterFlag, "stdout", "OpenTelemetry exporter")
	root.PersistentFlags().String(serverHttpBasicAuthFlag, "", "Http basic auth")

	viper.BindPFlags(root.PersistentFlags())
	viper.SetConfigName("numary")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.numary")
	viper.AddConfigPath("/etc/numary")
	viper.ReadInConfig()

	viper.SetEnvPrefix("numary")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()

	return root
}

func PrintVersion(cmd *cobra.Command, args []string) {
	fmt.Printf("Version: %s \n", Version)
	fmt.Printf("Date: %s \n", BuildDate)
	fmt.Printf("Commit: %s \n", Commit)
}

func createContainer(opts ...option) (*fx.App, error) {

	opts = append(opts,
		WithVersion(Version),
		WithOption(fx.Provide(func() (storage.Driver, error) {

			var (
				flavor             = sqlstorage.FlavorFromString(viper.GetString(storageDriverFlag))
				cached             bool
				connString         string
				connStringResolver sqlstorage.ConnStringResolver
			)
			switch flavor {
			case sqlstorage.PostgreSQL:
				cached = true
				connString = viper.GetString(storagePostgresConnectionStringFlagd)
			case sqlstorage.SQLite:
				connStringResolver = func(name string) string {
					return sqlstorage.SQLiteFileConnString(path.Join(
						viper.GetString(storageDirFlag),
						fmt.Sprintf("%s_%s.db", viper.GetString(storageSQLiteDBNameFlag), name),
					))
				}
			default:
				return nil, fmt.Errorf("Unknown storage driver: %s", viper.GetString(storageDirFlag))
			}

			if viper.GetBool(otelFlag) {
				sqlDriverName, err := otelsql.Register(
					sqlstorage.SQLDriverName(flavor),
					flavor.AttributeKeyValue().Value.AsString(),
				)
				if err != nil {
					return nil, fmt.Errorf("Error registering otel driver: %s", err)
				}
				sqlstorage.UpdateSQLDriverMapping(flavor, sqlDriverName)
			}

			var driver storage.Driver
			if cached {
				driver = sqlstorage.NewCachedDBDriver(flavor.String(), flavor, connString)
			} else {
				driver = sqlstorage.NewOpenCloseDBDriver(flavor.String(), flavor, connStringResolver)
			}

			return driver, nil
		})),
		WithCacheStorage(viper.GetBool(storageCacheFlag)),
		WithHttpBasicAuth(viper.GetString(serverHttpBasicAuthFlag)),
		WithLedgerLister(controllers.LedgerListerFn(func(*http.Request) []string {
			return viper.GetStringSlice(ledgersFlag)
		})),
		WithRememberConfig(true),
	)

	switch viper.GetString(otelExporterFlag) {
	case "stdout":
		opts = append(opts, WithOption(opentelemetry.StdoutModule()))
	case "jaeger":
		opts = append(opts, WithOption(opentelemetry.JaegerModule()))
	case "noop":
		opts = append(opts, WithOption(opentelemetry.NoOpModule()))
	}

	return NewContainer(opts...), nil
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
