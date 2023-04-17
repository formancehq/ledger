package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/numary/ledger/cmd/internal"
	"github.com/numary/ledger/pkg/redis"
	_ "github.com/numary/ledger/pkg/storage/sqlstorage/migrates/9-add-pre-post-volumes"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// Deprecated
	storageDirFlag = "storage.dir"
	// Deprecated
	storageDriverFlag = "storage.driver"
	// Deprecated
	storageSQLiteDBNameFlag             = "storage.sqlite.db_name"
	storagePostgresConnectionStringFlag = "storage.postgres.conn_string"
	// Deprecated
	storageCacheFlag                 = "storage.cache"
	serverHttpBindAddressFlag        = "server.http.bind_address"
	uiHttpBindAddressFlag            = "ui.http.bind_address"
	lockStrategyFlag                 = "lock-strategy"
	lockStrategyRedisUrlFlag         = "lock-strategy-redis-url"
	lockStrategyRedisDurationFlag    = "lock-strategy-redis-duration"
	lockStrategyRedisRetryFlag       = "lock-strategy-redis-retry"
	lockStrategyRedisTLSEnabledFlag  = "lock-strategy-redis-tls-enabled"
	lockStrategyRedisTLSInsecureFlag = "lock-strategy-redis-tls-insecure"

	authBearerEnabledFlag           = "auth-bearer-enabled"
	authBearerIntrospectUrlFlag     = "auth-bearer-introspect-url"
	authBearerAudienceFlag          = "auth-bearer-audience"
	authBearerAudiencesWildcardFlag = "auth-bearer-audiences-wildcard"
	authBearerUseScopesFlag         = "auth-bearer-use-scopes"

	commitPolicyFlag = "commit-policy"

	cacheCapacityBytes = "cache-capacity-bytes"
	cacheMaxNumKeys    = "cache-max-num-keys"
)

var (
	Version                = "develop"
	BuildDate              = "-"
	Commit                 = "-"
	DefaultSegmentWriteKey = ""
)

func NewRootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "numary",
		Short:             "Numary",
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if viper.GetString(storageDriverFlag) == "sqlite" {
				_, _ = fmt.Fprintln(os.Stderr,
					"WARNING: SQLite is being deprecated and will not be supported starting from the v2 of the Formance Ledger. Please use Postgres instead.")
			}
			err := os.MkdirAll(viper.GetString(storageDirFlag), 0700)
			if err != nil {
				return errors.Wrap(err, "creating storage directory")
			}
			return nil
		},
	}

	server := NewServer()
	version := NewVersion()
	start := NewServerStart()
	server.AddCommand(start)

	conf := NewConfig()
	conf.AddCommand(NewConfigInit())
	store := NewStorage()
	store.AddCommand(NewStorageInit())
	store.AddCommand(NewStorageList())
	store.AddCommand(NewStorageUpgrade())
	store.AddCommand(NewStorageScan())
	store.AddCommand(NewStorageDelete())

	scriptExec := NewScriptExec()
	scriptCheck := NewScriptCheck()

	root.AddCommand(server)
	root.AddCommand(conf)
	root.AddCommand(UICmd)
	root.AddCommand(store)
	root.AddCommand(scriptExec)
	root.AddCommand(scriptCheck)
	root.AddCommand(version)
	root.AddCommand(stickersCmd)

	root.AddCommand(NewDocCommand())

	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	root.PersistentFlags().Bool(service.DebugFlag, false, "Debug mode")
	root.PersistentFlags().String(storageDriverFlag, "sqlite", "Storage driver")
	if err := root.PersistentFlags().MarkDeprecated(storageDriverFlag,
		"SQLite is being deprecated and will not be supported starting from the v2 of the Formance Ledger. Only Postgres will be supported and this flag will be deprecated."); err != nil {
		panic(err)
	}
	root.PersistentFlags().String(storageDirFlag, path.Join(home, ".numary/data"), "Storage directory (for sqlite)")
	if err := root.PersistentFlags().MarkDeprecated(storageDirFlag,
		"SQLite is being deprecated and will not be supported starting from the v2 of the Formance Ledger. Only Postgres will be supported and this flag will be deprecated."); err != nil {
		panic(err)
	}
	root.PersistentFlags().String(storageSQLiteDBNameFlag, "numary", "SQLite database name")
	if err := root.PersistentFlags().MarkDeprecated(storageSQLiteDBNameFlag,
		"SQLite is being deprecated and will not be supported starting from the v2 of the Formance Ledger. Only Postgres will be supported and this flag will be deprecated."); err != nil {
		panic(err)
	}
	root.PersistentFlags().String(storagePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
	root.PersistentFlags().Bool(storageCacheFlag, true, "Storage cache")
	if err := root.PersistentFlags().MarkDeprecated(storageCacheFlag,
		"Storage cache is being deprecated and will not be supported from the v2 of the Formance Ledger."); err != nil {
		panic(err)
	}
	root.PersistentFlags().String(serverHttpBindAddressFlag, "localhost:3068", "API bind address")
	root.PersistentFlags().String(uiHttpBindAddressFlag, "localhost:3068", "UI bind address")
	root.PersistentFlags().String(lockStrategyFlag, "memory", "Lock strategy (memory, none, redis)")
	root.PersistentFlags().String(lockStrategyRedisUrlFlag, "", "Redis url when using redis locking strategy")
	root.PersistentFlags().Duration(lockStrategyRedisDurationFlag, redis.DefaultLockDuration, "Lock duration")
	root.PersistentFlags().Duration(lockStrategyRedisRetryFlag, redis.DefaultRetryInterval, "Retry lock period")
	root.PersistentFlags().Bool(lockStrategyRedisTLSEnabledFlag, false, "Use tls on redis")
	root.PersistentFlags().Bool(lockStrategyRedisTLSInsecureFlag, false, "Whether redis is trusted or not")
	root.PersistentFlags().Bool(authBearerEnabledFlag, false, "Enable bearer auth")
	root.PersistentFlags().String(authBearerIntrospectUrlFlag, "", "OAuth2 introspect URL")
	root.PersistentFlags().StringSlice(authBearerAudienceFlag, []string{}, "Allowed audiences")
	root.PersistentFlags().Bool(authBearerAudiencesWildcardFlag, false, "Don't check audience")
	root.PersistentFlags().Bool(authBearerUseScopesFlag, false, "Use scopes as defined by rfc https://datatracker.ietf.org/doc/html/rfc8693")
	root.PersistentFlags().String(commitPolicyFlag, "", "Transaction commit policy (default or allow-past-timestamps)")

	// 100 000 000 bytes is 100 MB
	root.PersistentFlags().Int(cacheCapacityBytes, 100000000, "Capacity in bytes of the cache storing Numscript in RAM")
	root.PersistentFlags().Int(cacheMaxNumKeys, 100, "Maximum number of Numscript to be stored in the cache in RAM")

	otlptraces.InitOTLPTracesFlags(root.PersistentFlags())
	internal.InitHTTPBasicFlags(root)
	internal.InitAnalyticsFlags(root, DefaultSegmentWriteKey)
	publish.InitCLIFlags(root)

	if err = viper.BindPFlags(root.PersistentFlags()); err != nil {
		panic(err)
	}

	viper.SetConfigName("numary")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.numary")
	viper.AddConfigPath("/etc/numary")
	if err = viper.ReadInConfig(); err != nil {
		fmt.Printf("loading config file: %s\n", err)
	}

	internal.BindEnv(viper.GetViper())

	return root
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		if _, err := fmt.Fprintln(os.Stderr, err); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}
