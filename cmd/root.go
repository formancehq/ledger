package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	storage2 "github.com/numary/ledger/pkg/storage"
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
			err := os.MkdirAll(viper.GetString("storage.dir"), 0700)
			if err != nil {
				return errors.Wrap(err, "creating storage directory")
			}

			if viper.GetBool("debug") {
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
					listener, err := net.Listen("tcp", viper.GetString("server.http.bind_address"))
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
				WithOption(fx.Invoke(func(storageFactory storage2.Factory) error {
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
					viper.Get("server.http.bind_address"),
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

	root.PersistentFlags().Bool("debug", false, "Debug mode")
	root.PersistentFlags().String("storage.driver", "sqlite", "Storage driver")
	root.PersistentFlags().String("storage.dir", path.Join(home, ".numary/data"), "Storage directory (for sqlite)")
	root.PersistentFlags().String("storage.sqlite.db_name", "numary", "SQLite database name")
	root.PersistentFlags().String("storage.postgres.conn_string", "postgresql://localhost/postgres", "Postgre connection string")
	root.PersistentFlags().Bool("storage.cache", true, "Storage cache")
	root.PersistentFlags().Bool("persist-config", true, "Persist config on disk")
	root.PersistentFlags().String("server.http.bind_address", "localhost:3068", "API bind address")
	root.PersistentFlags().String("ui.http.bind_address", "localhost:3068", "UI bind address")
	root.PersistentFlags().StringSlice("ledgers", []string{"quickstart"}, "Ledgers")

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
		WithOption(fx.Provide(func() (storage2.Driver, error) {
			switch viper.GetString("storage.driver") {
			case "sqlite":
				return sqlstorage.NewOpenCloseDBDriver("sqlite", sqlstorage.SQLite, func(name string) string {
					return sqlstorage.SQLiteFileConnString(path.Join(
						viper.GetString("storage.dir"),
						fmt.Sprintf("%s_%s.db", viper.GetString("storage.sqlite.db_name"), name),
					))
				}), nil
			case "postgres":
				return sqlstorage.NewCachedDBDriver("postgres", sqlstorage.PostgreSQL,
					viper.GetString("storage.postgres.conn_string")), nil
			default:
				return nil, fmt.Errorf("unknown storage driver %s", viper.GetString("storage.driver"))
			}
		})),
		WithCacheStorage(viper.GetBool("storage.cache")),
		WithHttpBasicAuth(viper.GetString("server.http.basic_auth")),
		WithLedgerLister(controllers.LedgerListerFn(func() []string {
			return viper.GetStringSlice("ledgers")
		})),
		WithRememberConfig(true),
	)

	return NewContainer(opts...), nil
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
