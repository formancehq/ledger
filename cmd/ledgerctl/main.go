package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cli"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing := cmdutil.SetupTracing(ctx, version.Get().Version)
	defer shutdownTracing(context.Background())

	ctx, span := cmdutil.StartRootSpan(ctx)
	defer span.End()

	err := rootCmd.ExecuteContext(ctx)
	if err != nil {
		cmdutil.RecordSpanError(span, err)

		if _, ok := errors.AsType[*cmdutil.CLIError](err); !ok {
			// Error was not already displayed — print it now.
			_, _ = fmt.Fprintln(rootCmd.ErrOrStderr(), err.Error())
		}

		return 1
	}

	return 0
}

func newRootCommand() *cobra.Command {
	return cli.NewCommand()
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
	"tls-server-name":     {},
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
