package main

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounttypes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/audit"
	authcmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/auth"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/chapters"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cluster"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/events"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/numscripts"
	profilecmd "github.com/formancehq/ledger/v3/cmd/ledgerctl/profile"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/provision"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/querycheckpoint"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/restore"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/signing"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/store"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/upgrade"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func main() {
	// run() owns all deferred cleanup (notably the OpenTelemetry span flush);
	// keeping os.Exit out here guarantees those defers run before the process
	// terminates, even on the error path.
	os.Exit(run())
}

func run() int {
	rootCmd := newRootCommand()
	rootCmd.SilenceErrors = true

	bindSubcommandEnv(rootCmd)

	// Initialise OpenTelemetry from the standard OTEL_* env vars. The root span
	// created here parents every per-RPC span emitted by the gRPC client handler,
	// so a single invocation produces one connected trace.
	ctx := context.Background()
	shutdownTracing := cmdutil.SetupTracing(ctx, version.Get().Version)
	defer shutdownTracing(context.Background())

	ctx, span := cmdutil.StartRootSpan(ctx)
	defer span.End()

	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		cmdutil.RecordSpanError(span, err)

		var cliErr *cmdutil.CLIError
		if !errors.As(err, &cliErr) {
			// Error was not already displayed — print it now.
			pterm.Error.Println(err.Error())
		}

		return 1
	}

	return 0
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ledgerctl",
		Short:        "Ledger v3 CLI client",
		Long:         "Command-line client for interacting with Ledger v3 servers via gRPC",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Refine the root span name now that the target subcommand is known
			// (e.g. "ledgerctl transactions get").
			cmdutil.NameCommandSpan(cmd)

			// Keep stdout reserved for machine-readable payloads when the
			// caller asked for --json or --yaml. Spinners, success messages,
			// errors — every pterm printer — gets redirected to stderr so a
			// downstream `jq`, `yq`, or K8s log scraper sees only the encoded
			// result on stdout (and the termination-log mirror we drop in
			// EncodeStructured).
			cmdutil.RoutePtermForStructuredOutput(cmd)

			// Resolve connection/security flags from the active profile and
			// environment. Shared with the shell-completion path, which must run
			// the same resolution because cobra skips PersistentPreRunE during
			// `__complete`.
			return cmdutil.ResolveConnectionFlags(cmd)
		},
	}

	// Add persistent flags for connection profiles.
	rootCmd.PersistentFlags().String("profile", "", "Connection profile name (env: LEDGERCTL_PROFILE)")
	_ = rootCmd.RegisterFlagCompletionFunc("profile", cmdutil.CompleteProfileNames)

	// Add persistent flags for server connection.
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address (env: LEDGERCTL_SERVER)")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS) (env: LEDGERCTL_INSECURE)")
	rootCmd.PersistentFlags().String("tls-ca-cert", "", "Path to CA certificate file (PEM) for server verification (env: LEDGERCTL_TLS_CA_CERT)")

	// Add persistent flags for request signing.
	rootCmd.PersistentFlags().String("signing-key", "", "Path to Ed25519 private key file (seed: 32 bytes raw or hex-encoded) (env: LEDGERCTL_SIGNING_KEY)")
	rootCmd.PersistentFlags().String("signing-key-id", "", "Key ID for request signatures (default: \"default\") (env: LEDGERCTL_SIGNING_KEY_ID)")

	// Add persistent flag for response signature verification.
	rootCmd.PersistentFlags().String("response-verify-key", "", "Path to Ed25519 seed file for verifying server response signatures (env: LEDGERCTL_RESPONSE_VERIFY_KEY)")

	// Add persistent flag for read consistency level.
	rootCmd.PersistentFlags().String("consistency", "", "Read consistency level: stale, leader, or linearizable (default) (env: LEDGERCTL_CONSISTENCY)")
	cmdutil.RegisterEnumCompletion(rootCmd, "consistency", "stale", "leader", "linearizable")

	// Add persistent flag for bearer token authentication.
	rootCmd.PersistentFlags().String("auth-token", "", "Bearer token for authentication (JWT string or @path-to-file) (env: LEDGERCTL_AUTH_TOKEN)")

	// Add persistent flag for an out-of-band sink of the --json result.
	// Generic on purpose: a CI wrapper, automation script, or the
	// ledger-operator (which points it at /dev/termination-log so the
	// kubelet captures the result on pod.status) can all opt in.
	rootCmd.PersistentFlags().String("result-file", "", "Also write the --json result to this file path (env: LEDGERCTL_RESULT_FILE)")

	// Add persistent flag for human-friendly amount display. Affects only the
	// rendered tables/text — structured (--json/--yaml) output keeps the raw
	// integer amounts and full "CUR/precision" asset strings so scripts stay
	// stable. Absent = no rescaling; --rescale alone = scale 0. The scale is a
	// uint8 (an asset's precision is a single byte), so pflag rejects values
	// above 255 at parse time.
	rootCmd.PersistentFlags().Uint8(cmdutil.RescaleFlagName, 0, "Re-express amounts at the given scale, summing same-currency balances across precisions (e.g. 1234 USD/2 + 56789 USD/3 → 69.129 USD; --rescale=2 → 6912.9 USD/2)")
	rootCmd.PersistentFlags().Lookup(cmdutil.RescaleFlagName).NoOptDefVal = "0"

	// Add subcommands.
	rootCmd.AddCommand(ledgers.NewCommand())
	rootCmd.AddCommand(indexes.NewCommand())
	rootCmd.AddCommand(accounts.NewCommand())
	rootCmd.AddCommand(accounttypes.NewCommand())
	rootCmd.AddCommand(transactions.NewCommand())
	rootCmd.AddCommand(store.NewCommand())
	rootCmd.AddCommand(cluster.NewCommand())
	rootCmd.AddCommand(audit.NewCommand())
	rootCmd.AddCommand(logs.NewCommand())
	rootCmd.AddCommand(signing.NewCommand())
	rootCmd.AddCommand(events.NewCommand())
	rootCmd.AddCommand(chapters.NewCommand())
	rootCmd.AddCommand(restore.NewCommand())
	rootCmd.AddCommand(authcmd.NewCommand())
	rootCmd.AddCommand(profilecmd.NewCommand())
	rootCmd.AddCommand(newVersionCommand())
	rootCmd.AddCommand(upgrade.NewCommand(version.Get().Version))
	rootCmd.AddCommand(provision.NewCommand())
	rootCmd.AddCommand(querycheckpoint.NewCommand())
	rootCmd.AddCommand(queries.NewCommand())
	rootCmd.AddCommand(numscripts.NewCommand())

	// Wire shell completion for every --ledger flag in the tree.
	registerLedgerFlagCompletion(rootCmd)

	return rootCmd
}

// registerLedgerFlagCompletion walks the command tree and attaches the
// ledger-name shell completion to every command exposing a --ledger flag
// (whether declared locally or inherited as a persistent flag).
//
// cobra keys completion functions by the flag pointer, so a command that only
// inherits the flag resolves to the same pointer as its declaring parent: the
// duplicate registration returns an "already registered" error, which we
// intentionally ignore. Registering once per declaring command is therefore
// sufficient and idempotent.
func registerLedgerFlagCompletion(cmd *cobra.Command) {
	if cmd.Flag("ledger") != nil {
		_ = cmd.RegisterFlagCompletionFunc("ledger", cmdutil.CompleteLedgerNames)
	}

	for _, sub := range cmd.Commands() {
		registerLedgerFlagCompletion(sub)
	}
}

// ledgerctlOwnedFlagNames are the profile/connection/security flags ledgerctl
// resolves exclusively through the LEDGERCTL_ prefix (root PersistentPreRunE for
// inherited flags, ResolveTokenSource for the token). They must never be bound to
// bare go-libs env names anywhere in the tree — including the subcommands that
// redeclare them locally (profile create, auth generate-token) — or a stray
// PROFILE/SERVER/SIGNING_KEY/INSECURE/… silently overrides the prefixed lookup.
//
// profile is included even though cobra's lazy persistent-flag merge means a
// subcommand flag set never exposes the inherited --profile at bind time (so
// bare PROFILE is not reachable today): membership makes the documented
// "only LEDGERCTL_PROFILE" contract true by construction rather than by an
// accident of merge ordering, and it stays correct if a subcommand ever declares
// a local --profile. EN-1295.
var ledgerctlOwnedFlagNames = map[string]struct{}{
	"profile":             {},
	"server":              {},
	"insecure":            {},
	"tls-ca-cert":         {},
	"consistency":         {},
	"auth-token":          {},
	"signing-key":         {},
	"signing-key-id":      {},
	"response-verify-key": {},
	"result-file":         {},
	// key-id is a per-command JWT/key identifier declared locally by
	// auth login / auth generate-token / signing register-key / signing
	// revoke-key. Skipping bare KEY_ID here keeps Changed("key-id") a
	// reliable "CLI-typed" signal, which auth's resolveKeyID uses to
	// prefer an explicit --signing-key-id over an env-derived KEY_ID.
	// Callers who need env-driven auth setup use LEDGERCTL_SIGNING_KEY_ID
	// (feeds --signing-key-id, which resolveKeyID falls back to).
	"key-id": {},
}

// bindSubcommandEnv binds bare-name environment variables (the go-libs
// convention, e.g. KEY_ID -> --key-id) to every subcommand flag EXCEPT the
// ledgerctl-owned connection/security flags. The root command itself is never
// bound: its persistent flags are owned and resolved with the LEDGERCTL_ prefix
// in PersistentPreRunE. main() and TestServerFlagEnvResolution share this
// helper so the test exercises the production wiring rather than re-implementing it.
func bindSubcommandEnv(rootCmd *cobra.Command) {
	for _, sub := range rootCmd.Commands() {
		bindEnvSkippingOwned(sub)
	}
}

// bindEnvSkippingOwned mirrors service.BindEnvToCommand's recursion but skips
// the ledgerctl-owned flag names so their bare env aliases are never honored.
func bindEnvSkippingOwned(cmd *cobra.Command) {
	bindFlagSetSkippingOwned(cmd.Flags())
	bindFlagSetSkippingOwned(cmd.PersistentFlags())

	for _, sub := range cmd.Commands() {
		bindEnvSkippingOwned(sub)
	}
}

// bindFlagSetSkippingOwned binds each non-owned flag in set to its bare
// uppercased env name (matching service.BindEnvToFlagSet, including the
// stringSlice space-to-comma handling used by flags such as --scopes).
func bindFlagSetSkippingOwned(set *pflag.FlagSet) {
	set.VisitAll(func(flag *pflag.Flag) {
		if _, owned := ledgerctlOwnedFlagNames[flag.Name]; owned {
			return
		}

		envVar := strings.ReplaceAll(strings.ToUpper(flag.Name), "-", "_")

		value := os.Getenv(envVar)
		if value == "" {
			return
		}

		value = strings.TrimSpace(value)
		if flag.Value.Type() == "stringSlice" && strings.Contains(value, " ") {
			value = strings.ReplaceAll(value, " ", ",")
		}

		// Ignore the error: an invalid env value leaves the cobra default in
		// place, matching resolveFlag's best-effort env handling.
		_ = set.Set(flag.Name, value)
	})
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "version",
		Short:             "Print version information",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		Run: func(_ *cobra.Command, _ []string) {
			banner, _ := pterm.DefaultBigText.WithLetters(
				putils.LettersFromStringWithStyle("Ledger", pterm.FgCyan.ToStyle()),
				putils.LettersFromStringWithStyle("ctl", pterm.FgLightMagenta.ToStyle()),
			).Srender()
			pterm.Println(banner)

			pterm.DefaultBox.WithTitle(pterm.LightGreen("Version Info")).
				Println(pterm.Sprintf("%s %s", pterm.LightCyan("Version:"), pterm.Green(version.Get().Version)))
		},
	}
}
