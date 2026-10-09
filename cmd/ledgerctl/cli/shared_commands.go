package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	ledgerplugin "github.com/formancehq/ledger/misc/fctl-plugin"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/shared"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	domainindexes "github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// These bindings preserve ledgerctl's public spelling. The product manifest
// defines the operations; this host owns aliases, input flags and rendering.
var sharedBindings = map[string][]string{
	"ledger/list":                         {"ledgers", "list"},
	"ledger/create":                       {"ledgers", "create"},
	"ledger/show":                         {"ledgers", "get"},
	"ledger/delete":                       {"ledgers", "delete"},
	"ledger/stats":                        {"ledgers", "stats"},
	"ledger/balances":                     {"accounts", "aggregate-volumes"},
	"ledger/info":                         {"info"},
	"ledger/bulk":                         {"bulk"},
	"ledger/metadata/show":                {"ledgers", "metadata", "show"},
	"ledger/metadata/set":                 {"ledgers", "set-metadata"},
	"ledger/metadata/delete":              {"ledgers", "delete-metadata"},
	"ledger/accounts/list":                {"accounts", "list"},
	"ledger/accounts/show":                {"accounts", "get"},
	"ledger/accounts/balances":            {"accounts", "balances"},
	"ledger/accounts/metadata/show":       {"accounts", "metadata", "show"},
	"ledger/accounts/metadata/set":        {"accounts", "set-metadata"},
	"ledger/accounts/metadata/delete":     {"accounts", "delete-metadata"},
	"ledger/transactions/list":            {"transactions", "list"},
	"ledger/transactions/show":            {"transactions", "get"},
	"ledger/transactions/create":          {"transactions", "create"},
	"ledger/transactions/revert":          {"transactions", "revert"},
	"ledger/transactions/metadata/show":   {"transactions", "metadata", "show"},
	"ledger/transactions/metadata/set":    {"transactions", "set-metadata"},
	"ledger/transactions/metadata/delete": {"transactions", "delete-metadata"},
	"ledger/indexes/list":                 {"indexes", "list"},
	"ledger/indexes/show":                 {"indexes", "show"},
	"ledger/indexes/status":               {"indexes", "status"},
	"ledger/indexes/create":               {"indexes", "create"},
	"ledger/indexes/delete":               {"indexes", "drop"},
	"ledger/indexes/inspect":              {"indexes", "inspect"},
	"ledger/logs/list":                    {"logs", "list"},
}

func attachSharedCommands(root *cobra.Command) {
	manifest, err := ledgerplugin.NewWithExecutor(version.Get().Version, nil).GetManifest(context.Background())
	if err != nil {
		panic(err)
	}
	var attach func(pluginsdk.CommandSpec, []string, []pluginsdk.FlagSpec)
	attach = func(spec pluginsdk.CommandSpec, parent []string, inherited []pluginsdk.FlagSpec) {
		path := append(append([]string{}, parent...), pluginsdk.CommandName(spec))
		flags := append(append([]pluginsdk.FlagSpec{}, inherited...), spec.Flags...)
		if spec.Runnable {
			nativePath, exists := sharedBindings[strings.Join(path, "/")]
			if !exists {
				panic("missing ledgerctl binding: " + strings.Join(path, "/"))
			}
			cmd := sharedCommandAt(root, nativePath, spec)
			addSharedFlags(cmd, flags)
			if cmd.Flag("json") == nil {
				cmdutil.AddOutputFlags(cmd)
			}
			if cmd.Flag("timeout") == nil {
				cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")
			}
			if cmd.Annotations == nil {
				cmd.Annotations = map[string]string{}
			}
			cmd.Annotations["ledger-command"] = strings.Join(path, "/")
			if strings.Join(path, "/") == "ledger/indexes/delete" {
				cmd.Use = "drop [index]"
				cmd.Args = cobra.MaximumNArgs(1)
			}
			if strings.Join(path, "/") == "ledger/indexes/inspect" {
				cmd.Use = "inspect [index]"
				cmd.Args = cobra.MaximumNArgs(1)
				if flag := cmd.Flag("key"); flag != nil {
					delete(flag.Annotations, cobra.BashCompOneRequiredFlag)
				}
			}
			if strings.Join(path, "/") == "ledger/create" || strings.Join(path, "/") == "ledger/show" {
				cmd.Args = cobra.MaximumNArgs(1)
			}
			cmd.RunE = func(cmd *cobra.Command, args []string) error { return runSharedCommand(cmd, spec, path, flags, args) }
		}
		for _, child := range spec.Subcommands {
			attach(child, path, flags)
		}
	}
	attach(manifest.Root, nil, nil)
}

func sharedCommandAt(root *cobra.Command, path []string, spec pluginsdk.CommandSpec) *cobra.Command {
	current := root
	for i, name := range path {
		var child *cobra.Command
		for _, candidate := range current.Commands() {
			if candidate.Name() == name {
				child = candidate

				break
			}
		}
		if child == nil {
			child = &cobra.Command{Use: name, Short: "Manage " + name, ValidArgsFunction: cobra.NoFileCompletions}
			if i == len(path)-1 {
				child.Use = name + strings.TrimPrefix(spec.Use, pluginsdk.CommandName(spec))
				child.Short, child.Long = spec.Short, spec.Long
				child.Args = cobra.MaximumNArgs(spec.Args.Max)
			}
			current.AddCommand(child)
		}
		current = child
	}

	return current
}

func addSharedFlags(cmd *cobra.Command, flags []pluginsdk.FlagSpec) {
	for _, spec := range flags {
		if cmd.Flag(spec.Name) != nil {
			continue
		}
		switch spec.Type {
		case "bool":
			value, _ := strconv.ParseBool(spec.Default)
			cmd.Flags().Bool(spec.Name, value, spec.Usage)
		case "uint32":
			value, _ := strconv.ParseUint(spec.Default, 10, 32)
			cmd.Flags().Uint32(spec.Name, uint32(value), spec.Usage)
		case "uint64":
			value, _ := strconv.ParseUint(spec.Default, 10, 64)
			cmd.Flags().Uint64(spec.Name, value, spec.Usage)
		default:
			cmd.Flags().String(spec.Name, spec.Default, spec.Usage)
		}
	}
}

func runSharedCommand(cmd *cobra.Command, spec pluginsdk.CommandSpec, path []string, flagSpecs []pluginsdk.FlagSpec, args []string) error {
	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	executor := &shared.Executor{Client: client}
	executor.CheckpointID, _ = cmd.Flags().GetUint64("checkpoint-id")
	executor.Consistency, _ = cmd.Flags().GetString("consistency")
	executor.Apply = func(ctx context.Context, key string, requests ...*servicepb.Request) (*servicepb.ApplyResponse, error) {
		request, err := cmdutil.BuildApplyRequestWithIdempotencyKey(cmd, key, requests...)
		if err != nil {
			return nil, err
		}
		response, err := client.Apply(ctx, request)
		if err != nil {
			return response, err
		}
		if err := cmdutil.VerifyResponseSignatures(cmd, response.GetLogs()); err != nil {
			return nil, fmt.Errorf("response signature verification failed: %w", err)
		}

		return response, nil
	}
	req := pluginsdk.ExecuteRequest{CommandPath: path, Args: append([]string{}, args...), Flags: map[string]string{}, ChangedFlags: map[string]bool{}, Context: map[string]string{}}
	for _, f := range flagSpecs {
		if flag := cmd.Flag(f.Name); flag != nil {
			req.Flags[f.Name], req.ChangedFlags[f.Name] = flag.Value.String(), flag.Changed
		}
	}
	for _, name := range []string{"prefix", "creation-key-prefix"} {
		if flag := cmd.Flag(name); flag != nil {
			req.Context[name] = flag.Value.String()
		}
	}
	if rescale := cmdutil.RescaleTarget(cmd); rescale != nil && !cmdutil.IsStructuredOutput(cmd) && strings.Join(path, "/") == "ledger/balances" {
		req.Flags["use-max-precision"] = "true"
	}
	if err := prepareSharedInput(cmdutil.CommandContextOrBackground(cmd), cmd, client, spec, &req, executor); err != nil {
		return err
	}
	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	if analyze, _ := cmd.Flags().GetBool("analyze"); analyze {
		ctx = cmdutil.ProfileContext(ctx)
	}
	plugin := ledgerplugin.NewWithExecutor(version.Get().Version, executor.Execute)

	return executeSharedPages(ctx, cmd, spec, plugin, req, executor)
}

func prepareSharedInput(ctx context.Context, cmd *cobra.Command, client servicepb.BucketServiceClient, spec pluginsdk.CommandSpec, req *pluginsdk.ExecuteRequest, executor *shared.Executor) error {
	path := strings.Join(req.CommandPath, "/")
	if req.Flags["after"] != "" && req.Flags["cursor"] != "" {
		return errors.New("--after and --cursor cannot be used together")
	}
	if len(req.Args) > 0 && (path == "ledger/indexes/delete" || path == "ledger/indexes/inspect") {
		for _, flag := range []string{"type", "target", "key"} {
			if cmd.Flags().Changed(flag) {
				return fmt.Errorf("positional index and --%s are mutually exclusive", flag)
			}
		}
	}
	if path == "ledger/create" || path == "ledger/show" || path == "ledger/delete" {
		names := []string{req.Flags["ledger"]}
		name, _ := cmd.Flags().GetString("name")
		names = append(names, name)
		if len(req.Args) > 0 {
			names = append(names, req.Args[0])
		}
		selected := ""
		for _, name := range names {
			if name == "" {
				continue
			}
			if selected != "" && selected != name {
				return errors.New("ledger names supplied by arguments, --name and --ledger must agree")
			}
			selected = name
		}
	}
	if err := rejectSharedBodyConflicts(cmd, path); err != nil {
		return err
	}
	switch {
	case path == "ledger/create" && !cmd.Flags().Changed("data"):
		name, _ := cmd.Flags().GetString("name")
		if name == "" {
			name = req.Flags["ledger"]
			if len(req.Args) > 0 {
				name = req.Args[0]
			}
			if name != "" {
				if err := cmd.Flags().Set("name", name); err != nil {
					return err
				}
			}
		}
		requests, err := ledgers.PrepareCreate(cmd)
		if err != nil {
			return err
		}
		executor.CreateLedgerRequests = requests
		req.Args = []string{requests[0].GetCreateLedger().GetName()}
		req.Body = json.RawMessage("{}")
	case path == "ledger/create" || path == "ledger/show" || path == "ledger/delete":
		name, _ := cmd.Flags().GetString("name")
		if len(req.Args) == 0 && name != "" {
			req.Args = []string{name}
		}
		if len(req.Args) == 0 && req.Flags["ledger"] != "" {
			req.Args = []string{req.Flags["ledger"]}
		}
		if len(req.Args) == 0 {
			if path == "ledger/create" {
				var err error
				name, err = sharedPrompt(ctx, cmd, "Ledger name")
				if err != nil {
					return err
				}
			} else {
				var err error
				name, err = sharedSelectLedger(ctx, cmd, client)
				if err != nil {
					return err
				}
			}
			if name == "" {
				return errors.New("ledger name is required (use --name or --ledger)")
			}
			req.Args = []string{name}
		}
	case path != "ledger/list" && path != "ledger/info":
		if req.Flags["ledger"] == "" {
			name, err := sharedSelectLedger(ctx, cmd, client)
			if err != nil {
				return err
			}
			req.Flags["ledger"] = name
		}
	}
	if path == "ledger/transactions/create" && !cmd.Flags().Changed("data") {
		payload, err := transactions.PrepareCreate(cmd, req.Flags["ledger"])
		if err != nil {
			return err
		}
		body, err := cmdutil.MarshalJSON(payload)
		if err != nil {
			return err
		}
		req.Body = body
	}
	if path == "ledger/indexes/inspect" {
		key, _ := cmd.Flags().GetString("key")
		target, _ := cmd.Flags().GetString("target")
		if len(req.Args) == 0 && key != "" {
			id, err := indexes.ParseDefinition("metadata:" + target + ":" + key)
			if err != nil {
				return err
			}
			req.Args = []string{domainindexes.Canonical(id)}
		}
		if req.Flags["mode"] == "distinct-values" {
			req.Flags["mode"] = "distinctValues"
		}
	}
	if path == "ledger/indexes/create" || path == "ledger/indexes/delete" {
		if (path == "ledger/indexes/delete" && len(req.Args) == 0) || (path == "ledger/indexes/create" && !cmd.Flags().Changed("data")) {
			id, err := sharedIndexID(cmd)
			if err != nil {
				return err
			}
			if path == "ledger/indexes/create" {
				req.Body, err = json.Marshal(map[string]string{"id": id})
				if err != nil {
					return err
				}
			} else {
				req.Args = []string{id}
			}
		}
	}
	for len(req.Args) < spec.Args.Min {
		value, err := sharedPrompt(ctx, cmd, "Required argument")
		if err != nil {
			return err
		}
		req.Args = append(req.Args, value)
	}
	if strings.HasSuffix(path, "/metadata/set") && !cmd.Flags().Changed("data") {
		entries, _ := cmd.Flags().GetStringArray("metadata")
		if len(entries) == 0 {
			value, err := sharedPrompt(ctx, cmd, "Metadata (key=value)")
			if err != nil {
				return err
			}
			entries = []string{value}
		}
		metadata := map[string]string{}
		for _, entry := range entries {
			key, value, err := cmdutil.ParseKeyValue(entry)
			if err != nil {
				return err
			}
			metadata[key] = value
		}
		body, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		req.Body = body
	}
	if path == "ledger/transactions/revert" && !cmd.Flags().Changed("data") {
		force, _ := cmd.Flags().GetBool("force")
		at, _ := cmd.Flags().GetBool("at-effective-date")
		metadata := map[string]string{}
		entries, _ := cmd.Flags().GetStringArray("metadata")
		for _, entry := range entries {
			key, value, err := cmdutil.ParseKeyValue(entry)
			if err != nil {
				return err
			}
			metadata[key] = value
		}
		body, err := json.Marshal(map[string]any{"force": force, "atEffectiveDate": at, "metadata": metadata})
		if err != nil {
			return err
		}
		req.Body = body
	}
	if cmd.Flags().Changed("data") {
		body, err := readSharedBody(ctx, cmd, req.Flags["data"])
		if err != nil {
			return err
		}
		req.Body = body
	}
	// Index drops historically execute directly in ledgerctl. The host supplies
	// the shared contract's confirmation after resolving the explicit index.
	if path == "ledger/indexes/delete" {
		req.Flags["confirm"], req.ChangedFlags["confirm"] = "true", true
	}
	if (spec.Confirm && path != "ledger/indexes/delete") || path == "ledger/transactions/revert" || strings.HasSuffix(path, "/metadata/delete") {
		yes, _ := cmd.Flags().GetBool("yes")
		confirm, _ := strconv.ParseBool(req.Flags["confirm"])
		if !yes && !confirm {
			value, err := sharedPrompt(ctx, cmd, "Confirm operation (yes/no)")
			if err != nil {
				return err
			}
			if value != "yes" && value != "y" {
				return errors.New("operation cancelled")
			}
		}
		if spec.Confirm {
			req.Flags["confirm"], req.ChangedFlags["confirm"] = "true", true
		}
	}

	return nil
}

func sharedIndexID(cmd *cobra.Command) (string, error) {
	kind, _ := cmd.Flags().GetString("type")
	if kind == "" {
		return "", errors.New("index type is required (use --type or --data)")
	}
	if kind != "metadata" {
		if cmd.Flags().Changed("target") || cmd.Flags().Changed("key") {
			return "", errors.New("--target and --key only apply to metadata indexes")
		}
		id, err := indexes.ParseDefinition(kind)
		if err != nil {
			return "", err
		}

		return domainindexes.Canonical(id), nil
	}
	target, _ := cmd.Flags().GetString("target")
	key, _ := cmd.Flags().GetString("key")
	if target == "" || key == "" {
		return "", errors.New("metadata indexes require --target and --key")
	}
	id, err := indexes.ParseDefinition("metadata:" + target + ":" + key)
	if err != nil {
		return "", err
	}

	return domainindexes.Canonical(id), nil
}

func rejectSharedBodyConflicts(cmd *cobra.Command, path string) error {
	if !cmd.Flags().Changed("data") {
		return nil
	}
	var incompatible []string
	switch path {
	case "ledger/create":
		incompatible = []string{"schema", "index", "default-enforcement-mode", "mode", "mirror-source-type", "mirror-ledger-name", "mirror-base-url", "mirror-oauth2-client-id", "mirror-oauth2-client-secret", "mirror-oauth2-token-endpoint", "mirror-oauth2-scopes", "mirror-dsn", "mirror-aws-iam-region", "mirror-aws-iam-assume-role-arn", "mirror-batch-size", "mirror-rewrite-file", "mirror-rewrite-rule"}
	case "ledger/transactions/create":
		incompatible = []string{"posting", "script", "var", "reference", "metadata", "force"}
	case "ledger/transactions/revert":
		incompatible = []string{"force", "at-effective-date", "metadata"}
	case "ledger/indexes/create":
		incompatible = []string{"type", "target", "key"}
	default:
		if strings.HasSuffix(path, "/metadata/set") {
			incompatible = []string{"metadata"}
		}
	}
	for _, name := range incompatible {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--data and --%s are mutually exclusive", name)
		}
	}

	return nil
}

func sharedSelectLedger(ctx context.Context, cmd *cobra.Command, client servicepb.BucketServiceClient) (string, error) {
	rpcCtx, cancel := cmdutil.GetContext(cmd)
	defer cancel()
	all, err := cmdutil.GetAllLedgersInfo(rpcCtx, client)
	if err != nil {
		return "", err
	}
	if len(all) == 1 {
		for name := range all {
			return name, nil
		}
	}
	if len(all) == 0 {
		return "", errors.New("no ledgers found; create one with ledgerctl ledgers create")
	}
	if !sharedInteractive(cmd) {
		return "", errors.New("several ledgers are available; select one with --ledger")
	}
	for name := range all {
		if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "  "+name); err != nil {
			return "", err
		}
	}
	name, err := sharedPrompt(ctx, cmd, "Select a ledger")
	if err != nil {
		return "", err
	}
	if all[name] == nil {
		return "", fmt.Errorf("unknown ledger %q", name)
	}

	return name, nil
}

func sharedInteractive(cmd *cobra.Command) bool {
	file, ok := cmd.InOrStdin().(*os.File)

	return ok && term.IsTerminal(int(file.Fd()))
}

func sharedPrompt(ctx context.Context, cmd *cobra.Command, title string) (string, error) {
	if !sharedInteractive(cmd) {
		return "", fmt.Errorf("%s is required in non-interactive mode", title)
	}
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s: ", title); err != nil {
		return "", err
	}
	line, err := readSharedBody(ctx, cmd, "-line")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(line))
	if value == "" {
		return "", fmt.Errorf("%s is required", title)
	}

	return value, nil
}

func readSharedBody(ctx context.Context, cmd *cobra.Command, value string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var reader io.Reader = strings.NewReader(value)
	if value == "-" || value == "-line" {
		reader = cmd.InOrStdin()
	}
	if after, ok := strings.CutPrefix(value, "@"); ok {
		file, err := os.Open(after)
		if err != nil {
			return nil, err
		}
		defer func() { _ = file.Close() }()
		reader = file
	}
	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		if value == "-line" {
			var line []byte
			one := make([]byte, 1)
			for len(line) <= 65536 {
				_, err := io.ReadFull(reader, one)
				if err != nil {
					done <- result{line, err}

					return
				}
				line = append(line, one[0])
				if one[0] == '\n' {
					done <- result{line, nil}

					return
				}
			}
			done <- result{nil, errors.New("input exceeds 64 KiB")}

			return
		}
		body, err := io.ReadAll(io.LimitReader(reader, (4<<20)+1))
		done <- result{body, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case read := <-done:
		if read.err != nil {
			return nil, read.err
		}
		if len(read.body) > 4<<20 {
			return nil, errors.New("JSON body exceeds 4 MiB")
		}
		if value != "-line" && !json.Valid(read.body) {
			return nil, errors.New("invalid JSON request body")
		}

		return read.body, nil
	}
}
