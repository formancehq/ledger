package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api"
	"github.com/numary/ledger/config"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/storage"
	"github.com/numary/machine/script/compiler"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"

	// These lines allow registering sql drivers using init() functions
	_ "github.com/numary/ledger/storage/postgres"
	_ "github.com/numary/ledger/storage/sqlite"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"

	root = &cobra.Command{
		Use:               "numary",
		Short:             "Numary",
		DisableAutoGenTag: true,
	}
)

func PrintVersion(cmd *cobra.Command, args []string) {
	fmt.Printf("Version: %s \n", Version)
	fmt.Printf("Date: %s \n", BuildDate)
	fmt.Printf("Commit: %s \n", Commit)
}

func Execute() {
	viper.SetDefault("version", Version)

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
		Run: func(cmd *cobra.Command, args []string) {

			config.Init()

			options := make([]interface{}, 0)
			if viper.GetBool("storage.cache") {
				options = append(options, fx.Annotate(
					storage.NewDefaultFactory,
					fx.ResultTags(`name:"underlyingStorage"`),
				))
				options = append(options, fx.Annotate(
					storage.NewCachedStorageFactory,
					fx.ParamTags(`name:"underlyingStorage"`),
					fx.As(new(storage.Factory)),
				))
				options = append(options, fx.Annotate(
					ledger.WithStorageFactory,
					fx.ResultTags(`group:"resolverOptions"`),
					fx.As(new(ledger.ResolverOption)),
				))
			} else {
				options = append(options, storage.NewDefaultFactory)
			}

			app := fx.New(
				fx.Provide(options...),
				fx.Provide(
					fx.Annotate(ledger.NewResolver, fx.ParamTags(`group:"resolverOptions"`)),
					api.NewAPI,
				),
				fx.Invoke(func(lc fx.Lifecycle, h *api.API, storageFactory storage.Factory) {
					lc.Append(fx.Hook{
						OnStop: func(ctx context.Context) error {
							log.Println("closing storage factory")
							err := storageFactory.Close(ctx)
							if err != nil {
								return errors.Wrap(err, "closing storage factory")
							}
							return nil
						},
					})
				}),
				api.Module,
			)

			app.Run()
		},
	}

	server.AddCommand(start)

	conf := &cobra.Command{
		Use: "config",
	}

	conf.AddCommand(&cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			config.Init()
			err := viper.SafeWriteConfig()
			if err != nil {
				fmt.Println(err)
			}
		},
	})

	store := &cobra.Command{
		Use: "storage",
	}

	store.AddCommand(&cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			config.Init()
			s, err := storage.GetStore("default")

			if err != nil {
				log.Fatal(err)
			}

			err = s.Initialize(context.Background())

			if err != nil {
				log.Fatal(err)
			}
		},
	})

	scriptExec := &cobra.Command{
		Use:  "exec [ledger] [script]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			config.Init()

			b, err := ioutil.ReadFile(args[1])

			if err != nil {
				log.Fatal(err)
			}

			r := regexp.MustCompile(`^\n`)
			s := string(b)
			s = r.ReplaceAllString(s, "")

			b, err = json.Marshal(gin.H{
				"plain": string(s),
			})

			if err != nil {
				log.Fatal(err)
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
				log.Fatal(err)
			}

			b, err = ioutil.ReadAll(res.Body)

			if err != nil {
				log.Fatal(err)
			}

			var result struct {
				Err string `json:"err,omitempty"`
				Ok  bool   `json:"ok"`
			}
			err = json.Unmarshal(b, &result)
			if err != nil {
				log.Fatal(err)
			}
			if result.Ok {
				fmt.Println("Script ran successfully ✅")
			} else {
				log.Fatal(result.Err)
			}
		},
	}

	scriptCheck := &cobra.Command{
		Use:  "check [script]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			config.Init()

			b, err := ioutil.ReadFile(args[0])

			if err != nil {
				log.Fatal(err)
			}

			_, err = compiler.Compile(string(b))
			if err != nil {
				log.Fatal(err)
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

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
