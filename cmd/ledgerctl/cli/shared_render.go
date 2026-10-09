package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/fctl/pkg/pluginsdk"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/accounts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/indexes"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/ledgers"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/logs"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/shared"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	domainindexes "github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func executeSharedPages(ctx context.Context, cmd *cobra.Command, spec pluginsdk.CommandSpec, plugin pluginsdk.Plugin, req pluginsdk.ExecuteRequest, executor *shared.Executor) error {
	path := strings.Join(req.CommandPath, "/")
	// Reuse the host's pager and presentation while fetching every page through
	// the shared operation. The callback receives fresh per-page timeout contexts.
	fetch := func(pageCtx context.Context, page cmdutil.PaginationFlags) (any, metadata.MD, error) {
		pageRequest := req
		pageRequest.Flags = make(map[string]string, len(req.Flags))
		maps.Copy(pageRequest.Flags, req.Flags)
		pageRequest.ChangedFlags = make(map[string]bool, len(req.ChangedFlags))
		maps.Copy(pageRequest.ChangedFlags, req.ChangedFlags)
		if page.Cursor != "" {
			delete(pageRequest.Flags, "after")
			delete(pageRequest.ChangedFlags, "after")
		}
		pageRequest.Flags["cursor"] = page.Cursor
		if page.PageSize != 0 {
			pageRequest.Flags["page-size"] = strconv.FormatUint(uint64(page.PageSize), 10)
		}
		pageRequest.Flags["reverse"] = strconv.FormatBool(page.Reverse)
		_, err := plugin.Execute(pageCtx, pageRequest)

		return executor.Result, executor.Trailer, err
	}
	switch path {
	case "ledger/accounts/list":
		return accounts.RunListWithFetch(cmd, func(ctx context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Account, metadata.MD, error) {
			result, trailer, err := fetch(ctx, page)
			items, _ := result.([]*commonpb.Account)

			return items, trailer, err
		})
	case "ledger/transactions/list":
		return transactions.RunListWithFetch(cmd, req.Flags["ledger"], func(ctx context.Context, page cmdutil.PaginationFlags) ([]*commonpb.Transaction, metadata.MD, error) {
			result, trailer, err := fetch(ctx, page)
			items, _ := result.([]*commonpb.Transaction)

			return items, trailer, err
		})
	}
	drain, _ := cmd.Flags().GetBool("all")
	if path == "ledger/list" && !cmd.Flags().Changed("page-size") && !cmd.Flags().Changed("cursor") {
		drain = true
	}
	visited := map[string]bool{}
	var accumulated any
	for {
		cursor := req.Flags["cursor"]
		if visited[cursor] {
			return fmt.Errorf("pagination returned a repeated cursor %q", cursor)
		}
		visited[cursor] = true
		response, executeErr := plugin.Execute(ctx, req)
		if drain && executor.Result != nil {
			value := reflect.ValueOf(executor.Result)
			if value.Kind() != reflect.Slice {
				return errors.New("pagination requires a list response")
			}
			if accumulated == nil {
				accumulated = executor.Result
			} else {
				accumulated = reflect.AppendSlice(reflect.ValueOf(accumulated), value).Interface()
			}
		}
		if executeErr != nil || !drain || executor.NextCursor == "" {
			if accumulated != nil {
				executor.Result = accumulated
			}
			if drain && executeErr == nil {
				executor.NextCursor, executor.PreviousCursor = "", ""
			}
			if executor.Result != nil || len(response.Data) != 0 {
				renderErr := renderSharedNative(cmd, req, executor, response)

				return errors.Join(executeErr, renderErr)
			}

			return executeErr
		}
		delete(req.Flags, "after")
		delete(req.ChangedFlags, "after")
		req.Flags["cursor"] = executor.NextCursor
	}
}

func renderSharedNative(cmd *cobra.Command, req pluginsdk.ExecuteRequest, executor *shared.Executor, response pluginsdk.ExecuteResponse) (renderErr error) {
	path := strings.Join(req.CommandPath, "/")
	cursors := cmdutil.PageCursors{Next: executor.NextCursor, Previous: executor.PreviousCursor}
	defer func() {
		if analyze, _ := cmd.Flags().GetBool("analyze"); analyze && renderErr == nil && executor.Trailer != nil {
			writer := cmd.OutOrStdout()
			if cmdutil.IsStructuredOutput(cmd) {
				writer = cmd.ErrOrStderr()
			}
			cmdutil.RenderProfileTo(writer, cmdutil.ExtractProfile(executor.Trailer))
		}
	}()
	if strings.HasSuffix(path, "/metadata/set") {
		data := map[string]any{"metadata": req.Body}
		switch path {
		case "ledger/metadata/set":
			data["ledger"] = req.Flags["ledger"]
		case "ledger/accounts/metadata/set":
			data["address"] = req.Args[0]
		case "ledger/transactions/metadata/set":
			id, err := strconv.ParseUint(req.Args[0], 10, 64)
			if err != nil {
				return err
			}
			data["transactionId"] = id
		}
		if handled, err := cmdutil.EncodeStructured(cmd, data); handled || err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(req.Body, &fields); err != nil {
			return err
		}
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		table := pterm.TableData{{"KEY", "VALUE"}}
		for _, key := range keys {
			table = append(table, []string{key, string(fields[key])})
		}

		return pterm.DefaultTable.WithWriter(cmd.OutOrStdout()).WithHasHeader().WithData(table).Render()
	}
	switch path {
	case "ledger/create":
		return ledgers.RenderCreate(cmd, executor.LastApplyResponse)
	case "ledger/transactions/create":
		return transactions.RenderCreate(cmd, executor.LastApplyResponse)
	}
	switch result := executor.Result.(type) {
	case []*commonpb.LedgerInfo:
		return ledgers.RenderList(cmd, result, cursors)
	case *commonpb.LedgerInfo:
		return ledgers.RenderGet(cmd, result)
	case *commonpb.LedgerStats:
		return ledgers.RenderStats(cmd, req.Flags["ledger"], result)
	case *commonpb.DeletedLedgerLog:
		return ledgers.RenderDelete(cmd, result)
	case *commonpb.Account:
		return accounts.RenderGet(cmd, result)
	case []*commonpb.Account:
		return accounts.RenderList(cmd, result, cursors)
	case *servicepb.GetTransactionResponse:
		return transactions.RenderGet(cmd, result)
	case []*commonpb.Transaction:
		return transactions.RenderList(cmd, result, cursors)
	case *commonpb.RevertedTransaction:
		return transactions.RenderRevert(cmd, result)
	case *commonpb.AggregateResult:
		return accounts.RenderAggregate(cmd, result)
	case []*commonpb.Log:
		return logs.RenderList(cmd, result, cursors)
	case []*commonpb.Index:
		ctx, cancel := cmdutil.GetContext(cmd)
		defer cancel()
		status, err := executor.Client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: req.Flags["ledger"]})
		if err != nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Index readiness unavailable: %v\n", err)
		}

		return indexes.RenderList(cmd, req.Flags["ledger"], result, status)
	case *servicepb.InspectIndexResponse:
		id, err := domainindexes.ParseCanonical(req.Args[0])
		if err != nil {
			return err
		}
		entry := id.GetMetadata()
		options := indexes.InspectRenderOptions{Ledger: req.Flags["ledger"], Key: entry.GetKey(), Target: cmdutil.TargetTypeString(entry.GetTarget()), DeclaredType: executor.InspectMetadataType}

		return indexes.RenderInspect(cmd, result, options)
	}
	data := executor.Result
	if data == nil {
		decoder := json.NewDecoder(bytes.NewReader(response.Data))
		decoder.UseNumber()
		if err := decoder.Decode(&data); err != nil {
			return err
		}
	}
	if handled, err := cmdutil.EncodeStructured(cmd, data); handled || err != nil {
		return err
	}
	// New host commands with no legacy presentation still keep exact JSON
	// values; existing commands retain their specialized tables above.
	output, err := cmdutil.MarshalJSON(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), pterm.FgCyan.Sprint(cmd.CommandPath())+"\n"+string(output))
	cmdutil.EmitCursorHints(cmd, cursors)

	return err
}
