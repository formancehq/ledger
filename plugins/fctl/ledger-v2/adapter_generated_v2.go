package ledgerv2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/producthttp"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"gopkg.in/yaml.v3"
)

type generatedV2 struct{ client *ledgerclient.Formance }

func newGeneratedV2(host sdk.Host, operation sdk.OperationPolicy) (*generatedV2, error) {
	bound, err := producthttp.New(host, operation, authStackCapability)
	if err != nil {
		return nil, err
	}
	return &generatedV2{client: ledgerclient.New(
		ledgerclient.WithClient(stripGeneratedNullGETBody{next: bound}),
		ledgerclient.WithServerURL("https://product.invalid"),
		ledgerclient.WithSecuritySource(func(context.Context) (components.Security, error) {
			return components.Security{}, nil
		}),
	)}, nil
}

// stripGeneratedNullGETBody repairs two request-shaping defects in the pinned
// Speakeasy output. It applies two independent rules, and the name reflects only
// the first; both are stated here because the second is the broader one.
//
// Body rule: Speakeasy marks several optional Ledger-v2 GET filter bodies as
// required and serializes nil as the byte-exact JSON `null`. Only that exact
// artificial value is removed. Real filter bodies and all non-GET bodies remain
// generated-client owned.
//
// Query rule: any GET carrying a non-empty `cursor` is canonicalized to that
// cursor alone. This is deliberately broader than the one operation that
// prompted it, because the generated client injects its own defaults beside a
// continuation cursor on more than one operation — `V2ListLedgers` adds
// `includeDeleted=false`, `V2ListSchemas` adds `order`, `pageSize` and `sort`.
// The rule is safe only while every such parameter is a client-side default the
// adapter never asked for, which is the case at the pinned client: the adapter
// sets page size, sort and filters on the first page only, so a continuation
// request carries no caller intent beyond the cursor.
// TestGeneratedClientSendsOnlyTheCursorOnEveryPaginatedContinuation pins that
// precondition, so a regenerated client that combines a cursor with a real
// parameter fails a test instead of having the parameter silently dropped.
type stripGeneratedNullGETBody struct{ next ledgerclient.HTTPClient }

func (c stripGeneratedNullGETBody) Do(request *http.Request) (*http.Response, error) {
	if request != nil && request.Method == http.MethodGet {
		if cursor := request.URL.Query().Get("cursor"); cursor != "" {
			query := request.URL.Query()
			for key := range query {
				query.Del(key)
			}
			query.Set("cursor", cursor)
			request.URL.RawQuery = query.Encode()
		}
	}
	if request != nil && request.Method == http.MethodGet && request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		if err := request.Body.Close(); err != nil {
			return nil, err
		}
		if bytes.Equal(body, []byte("null")) {
			request.Body = http.NoBody
			request.ContentLength = 0
			request.GetBody = nil
			request.Header.Del("Content-Type")
		} else {
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
		}
	}
	return c.next.Do(request)
}

func executeGeneratedV2(ctx context.Context, request sdk.ExecuteRequest, host sdk.Host) error {
	command, ok := commandByID(request.CommandID)
	if !ok {
		return invalidArgument("unknown command %q", request.CommandID)
	}
	if err := sdk.ValidateExecuteRequest(command, request); err != nil {
		return invalidArgument("invalid execution request: %s", err)
	}
	if err := sdk.ValidateTargetSelection(command.Target, request.Target); err != nil {
		return invalidArgument("invalid target: %s", err)
	}
	flags, err := collectFlags(command, request.Flags)
	if err != nil {
		return err
	}
	if len(command.Operations) == 0 {
		return descriptorInvalid("command %q declares no operation", command.ID)
	}
	generated, err := newGeneratedV2(host, command.Operations[0])
	if err != nil {
		return fmt.Errorf("ledger-v2: configure generated client: %w", err)
	}
	v2 := generated.client.Ledger.V2
	ledger := first(flags["ledger"])
	idempotency := optionalString(first(flags["idempotency-key"]))

	switch command.ID {
	case "ledger.v2.list":
		return executeListLedgers(ctx, v2, command, request.Continuation, host)
	case "ledger.v2.create":
		metadata, err := parseMetadata(flags["metadata"])
		if err != nil {
			return err
		}
		features, err := parseMetadata(flags["features"])
		if err != nil {
			return err
		}
		_, err = v2.CreateLedger(ctx, operations.V2CreateLedgerRequest{Ledger: request.Arguments[0], V2CreateLedgerRequest: components.V2CreateLedgerRequest{Bucket: optionalString(first(flags["bucket"])), Metadata: metadata, Features: features}})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.set-metadata":
		metadata, err := parseMetadata(request.Arguments[1:])
		if err != nil {
			return err
		}
		_, err = v2.UpdateLedgerMetadata(ctx, operations.V2UpdateLedgerMetadataRequest{Ledger: request.Arguments[0], RequestBody: metadata})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.delete-metadata":
		_, err = v2.DeleteLedgerMetadata(ctx, operations.V2DeleteLedgerMetadataRequest{Ledger: request.Arguments[0], Key: request.Arguments[1]})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.export":
		response, err := v2.ExportLogs(ctx, operations.V2ExportLogsRequest{Ledger: ledger})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.Bytes == nil {
			return generatedMissing(command)
		}
		data, readErr := io.ReadAll(response.Bytes)
		closeErr := response.Bytes.Close()
		if readErr != nil || closeErr != nil {
			return fmt.Errorf("ledger-v2: read export result")
		}
		return emitBytes(host, command.ID, sdk.ResultObject, mediaTypeOctetStream, data, nil)
	case "ledger.v2.import":
		body, err := readInputArtifact(ctx, host, request.Arguments[1], inputArtifactBytes)
		if err != nil {
			return err
		}
		resume := first(flags["resume-from-last-log"]) == "true"
		if resume {
			body, err = resumeGeneratedImport(ctx, command, request.Arguments[0], body, host)
			if err != nil {
				return err
			}
			if len(body) == 0 {
				return emitBytes(host, command.ID, sdk.ResultObject, mediaTypeJSON, []byte(`{}`), nil)
			}
		}
		batches, err := splitImportBatches(body)
		if err != nil {
			return err
		}
		if command.ExecutionPolicy == nil {
			return descriptorInvalid("command %q does not declare its request budget", command.ID)
		}
		availableRequests := int(command.ExecutionPolicy.MaxHostRequests)
		if resume {
			availableRequests--
		}
		if len(batches) > availableRequests {
			return budgetExhausted("import exceeds the host-request ceiling")
		}
		for _, batch := range batches {
			if _, err := v2.ImportLogs(ctx, operations.V2ImportLogsRequest{Ledger: request.Arguments[0], V2ImportLogsRequest: batch}); err != nil {
				return generatedError(command, err)
			}
		}
		return emitBytes(host, command.ID, sdk.ResultObject, mediaTypeJSON, []byte(`{}`), nil)
	case "ledger.v2.stats":
		response, err := v2.ReadStats(ctx, operations.V2ReadStatsRequest{Ledger: ledger})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.V2StatsResponse == nil {
			return generatedMissing(command)
		}
		return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2StatsResponse.Data, nil)
	case "ledger.v2.send":
		metadata, err := parseMetadata(flags["metadata"])
		if err != nil {
			return err
		}
		amount, err := parseBigInt(request.Arguments[1])
		if err != nil {
			return err
		}
		source := first(flags["source"])
		if source == "" {
			source = "world"
		}
		post := components.V2PostTransaction{Metadata: metadata, Reference: optionalString(first(flags["reference"])), Postings: []components.V2Posting{{Amount: amount, Asset: request.Arguments[2], Destination: request.Arguments[0], Source: source}}}
		return executeCreateTransaction(ctx, v2, command, operations.V2CreateTransactionRequest{Ledger: ledger, IdempotencyKey: idempotency, V2PostTransaction: post}, host)
	case "ledger.v2.accounts.list":
		return executeListAccounts(ctx, v2, command, flags, request.Continuation, host)
	case "ledger.v2.accounts.show":
		response, err := v2.GetAccount(ctx, operations.V2GetAccountRequest{Ledger: ledger, Address: request.Arguments[0], Expand: optionalString("volumes")})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.V2AccountResponse == nil {
			return generatedMissing(command)
		}
		return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2AccountResponse.Data, nil)
	case "ledger.v2.accounts.set-metadata":
		metadata, err := parseMetadata(request.Arguments[1:])
		if err != nil {
			return err
		}
		_, err = v2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{Ledger: ledger, Address: request.Arguments[0], IdempotencyKey: idempotency, RequestBody: metadata})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.accounts.delete-metadata":
		_, err = v2.DeleteAccountMetadata(ctx, operations.V2DeleteAccountMetadataRequest{Ledger: ledger, Address: request.Arguments[0], Key: request.Arguments[1], IdempotencyKey: idempotency})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.transactions.list":
		return executeListTransactions(ctx, v2, command, flags, request.Continuation, host)
	case "ledger.v2.transactions.show":
		id, err := resolveTransactionID(ctx, command, ledger, request.Arguments[0], host)
		if err != nil {
			return err
		}
		response, err := v2.GetTransaction(ctx, operations.V2GetTransactionRequest{Ledger: ledger, ID: id, Expand: optionalString("volumes")})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.V2GetTransactionResponse == nil {
			return generatedMissing(command)
		}
		return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2GetTransactionResponse.Data, nil)
	case "ledger.v2.transactions.num":
		body, err := numscriptTransaction(ctx, request, flags, host)
		if err != nil {
			return err
		}
		return executeCreateTransaction(ctx, v2, command, operations.V2CreateTransactionRequest{Ledger: ledger, IdempotencyKey: idempotency, V2PostTransaction: body}, host)
	case "ledger.v2.transactions.set-metadata":
		id, err := resolveTransactionID(ctx, command, ledger, request.Arguments[0], host)
		if err != nil {
			return err
		}
		metadata, err := parseMetadata(request.Arguments[1:])
		if err != nil {
			return err
		}
		_, err = v2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{Ledger: ledger, ID: id, IdempotencyKey: idempotency, RequestBody: metadata})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.transactions.delete-metadata":
		id, err := resolveTransactionID(ctx, command, ledger, request.Arguments[0], host)
		if err != nil {
			return err
		}
		_, err = v2.DeleteTransactionMetadata(ctx, operations.V2DeleteTransactionMetadataRequest{Ledger: ledger, ID: id, Key: request.Arguments[1], IdempotencyKey: idempotency})
		return emitGeneratedEmpty(host, command, err)
	case "ledger.v2.transactions.revert":
		id, err := resolveTransactionID(ctx, command, ledger, request.Arguments[0], host)
		if err != nil {
			return err
		}
		force, err := optionalBool(first(flags["force"]))
		if err != nil {
			return err
		}
		atDate, err := optionalBool(first(flags["at-effective-date"]))
		if err != nil {
			return err
		}
		response, err := v2.RevertTransaction(ctx, operations.V2RevertTransactionRequest{Ledger: ledger, ID: id, Force: force, AtEffectiveDate: atDate, IdempotencyKey: idempotency, V2RevertTransactionRequest: &components.V2RevertTransactionRequest{}})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.V2RevertTransactionResponse == nil {
			return generatedMissing(command)
		}
		return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2RevertTransactionResponse.Data, nil)
	case "ledger.v2.volumes.list":
		return executeListVolumes(ctx, v2, command, flags, request.Continuation, host)
	case "ledger.v2.schemas.list":
		return executeListSchemas(ctx, v2, command, flags, request.Continuation, host)
	case "ledger.v2.schemas.get":
		response, err := v2.GetSchema(ctx, operations.V2GetSchemaRequest{Ledger: ledger, Version: request.Arguments[0]})
		if err != nil {
			return generatedError(command, err)
		}
		if response == nil || response.V2SchemaResponse == nil {
			return generatedMissing(command)
		}
		return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2SchemaResponse.Data, nil)
	case "ledger.v2.schemas.insert":
		source, err := readInputArtifact(ctx, host, request.Arguments[1], requestJSONBytes)
		if err != nil {
			return err
		}
		schema, err := parseSchemaData(source)
		if err != nil {
			return err
		}
		_, err = v2.InsertSchema(ctx, operations.V2InsertSchemaRequest{Ledger: ledger, Version: request.Arguments[0], IdempotencyKey: idempotency, V2SchemaData: schema})
		return emitGeneratedEmpty(host, command, err)
	default:
		return sdk.Failure{Code: string(sdk.FailureOperationNotPermitted), Message: "ledger-v2: command is not implemented"}
	}
}

func executeCreateTransaction(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, request operations.V2CreateTransactionRequest, host sdk.Host) error {
	response, err := v2.CreateTransaction(ctx, request)
	if err != nil {
		return generatedError(command, err)
	}
	if response == nil || response.V2CreateTransactionResponse == nil {
		return generatedMissing(command)
	}
	return emitGeneratedJSON(host, command, sdk.ResultObject, response.V2CreateTransactionResponse.Data, nil)
}

func executeListLedgers(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, control sdk.ContinuationControl, host sdk.Host) error {
	items, page, err := collectGeneratedPages(control, nil, func(cursor *string) ([]components.V2Ledger, *string, bool, error) {
		response, err := v2.ListLedgers(ctx, operations.V2ListLedgersRequest{Cursor: cursor})
		if err != nil {
			return nil, nil, false, generatedError(command, err)
		}
		if response == nil || response.V2LedgerListResponse == nil {
			return nil, nil, false, generatedMissing(command)
		}
		value := response.V2LedgerListResponse.Cursor
		return value.Data, value.Next, value.HasMore, nil
	})
	if err != nil {
		return err
	}
	return emitGeneratedJSON(host, command, sdk.ResultCollection, items, page)
}

func executeListAccounts(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, flags map[string][]string, control sdk.ContinuationControl, host sdk.Host) error {
	body, err := filterBody(flags)
	if err != nil {
		return err
	}
	initial := optionalString(first(flags["cursor"]))
	items, page, err := collectGeneratedPages(control, initial, func(cursor *string) ([]components.V2Account, *string, bool, error) {
		req := operations.V2ListAccountsRequest{Ledger: first(flags["ledger"]), Cursor: cursor}
		if cursor == nil {
			req.PageSize = optionalInt64(first(flags["page-size"]))
			req.RequestBody = body
		}
		response, err := v2.ListAccounts(ctx, req)
		if err != nil {
			return nil, nil, false, generatedError(command, err)
		}
		if response == nil || response.V2AccountsCursorResponse == nil {
			return nil, nil, false, generatedMissing(command)
		}
		value := response.V2AccountsCursorResponse.Cursor
		return value.Data, value.Next, value.HasMore, nil
	})
	if err != nil {
		return err
	}
	return emitGeneratedJSON(host, command, sdk.ResultCollection, items, page)
}

func executeListTransactions(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, flags map[string][]string, control sdk.ContinuationControl, host sdk.Host) error {
	body, err := filterBody(flags)
	if err != nil {
		return err
	}
	initial := optionalString(first(flags["cursor"]))
	items, page, err := collectGeneratedPages(control, initial, func(cursor *string) ([]components.V2Transaction, *string, bool, error) {
		req := operations.V2ListTransactionsRequest{Ledger: first(flags["ledger"]), Cursor: cursor}
		if cursor == nil {
			req.PageSize = optionalInt64(first(flags["page-size"]))
			req.RequestBody = body
		}
		response, err := v2.ListTransactions(ctx, req)
		if err != nil {
			return nil, nil, false, generatedError(command, err)
		}
		if response == nil || response.V2TransactionsCursorResponse == nil {
			return nil, nil, false, generatedMissing(command)
		}
		value := response.V2TransactionsCursorResponse.Cursor
		return value.Data, value.Next, value.HasMore, nil
	})
	if err != nil {
		return err
	}
	return emitGeneratedJSON(host, command, sdk.ResultCollection, items, page)
}

func executeListVolumes(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, flags map[string][]string, control sdk.ContinuationControl, host sdk.Host) error {
	body, err := filterBody(flags)
	if err != nil {
		return err
	}
	insertionDate, err := optionalBool(first(flags["insertion-date"]))
	if err != nil {
		return err
	}
	startTime, err := optionalTime(first(flags["start-time"]))
	if err != nil {
		return err
	}
	endTime, err := optionalTime(first(flags["end-time"]))
	if err != nil {
		return err
	}
	initial := optionalString(first(flags["cursor"]))
	items, page, err := collectGeneratedPages(control, initial, func(cursor *string) ([]components.V2VolumesWithBalance, *string, bool, error) {
		req := operations.V2GetVolumesWithBalancesRequest{Ledger: first(flags["ledger"]), Cursor: cursor}
		if cursor == nil {
			req.PageSize = optionalInt64(first(flags["page-size"]))
			req.GroupBy = optionalInt64(first(flags["group-by"]))
			req.InsertionDate = insertionDate
			req.StartTime = startTime
			req.EndTime = endTime
			req.RequestBody = body
		}
		response, err := v2.GetVolumesWithBalances(ctx, req)
		if err != nil {
			return nil, nil, false, generatedError(command, err)
		}
		if response == nil || response.V2VolumesWithBalanceCursorResponse == nil {
			return nil, nil, false, generatedMissing(command)
		}
		value := response.V2VolumesWithBalanceCursorResponse.Cursor
		return value.Data, value.Next, value.HasMore, nil
	})
	if err != nil {
		return err
	}
	return emitGeneratedJSON(host, command, sdk.ResultCollection, items, page)
}

func executeListSchemas(ctx context.Context, v2 *ledgerclient.V2, command sdk.Command, flags map[string][]string, control sdk.ContinuationControl, host sdk.Host) error {
	initial := optionalString(first(flags["cursor"]))
	items, page, err := collectGeneratedPages(control, initial, func(cursor *string) ([]components.V2Schema, *string, bool, error) {
		req := operations.V2ListSchemasRequest{Ledger: first(flags["ledger"]), Cursor: cursor}
		if cursor == nil {
			req.PageSize = optionalInt64(first(flags["page-size"]))
		}
		response, err := v2.ListSchemas(ctx, req)
		if err != nil {
			return nil, nil, false, generatedError(command, err)
		}
		if response == nil || response.V2SchemasCursorResponse == nil {
			return nil, nil, false, generatedMissing(command)
		}
		value := response.V2SchemasCursorResponse.Cursor
		return value.Data, value.Next, value.HasMore, nil
	})
	if err != nil {
		return err
	}
	return emitGeneratedJSON(host, command, sdk.ResultCollection, items, page)
}

func collectGeneratedPages[T any](control sdk.ContinuationControl, initial *string, fetch func(*string) ([]T, *string, bool, error)) ([]T, *sdk.PageInfo, error) {
	all := control.Mode == sdk.ContinuationAllPages
	maxPages := uint32(1)
	if all {
		maxPages = control.MaxPages
	}
	items := make([]T, 0)
	// encodedBytes is the accumulation's marshalled length without its two
	// enclosing brackets; see the incremental update below.
	var encodedBytes uint64
	cursor := initial
	seen := map[string]struct{}{}
	for pageNumber := uint32(0); pageNumber < maxPages; pageNumber++ {
		pageItems, next, hasMore, err := fetch(cursor)
		if err != nil {
			return nil, nil, err
		}
		// The consistency rule is the same in both modes: hasMore is true
		// exactly when a next cursor is present. Enforcing it only on the
		// single-page path would make one inconsistent server response a hard
		// error and the identical response a silent traversal under --all.
		value := ""
		if next != nil {
			value = *next
		}
		if hasMore != (value != "") {
			return nil, nil, fmt.Errorf("ledger-v2: invalid cursor response")
		}
		if !all {
			return pageItems, &sdk.PageInfo{NextCursor: value, HasMore: hasMore}, nil
		}
		// Track the accumulation's encoded size incrementally. Re-marshalling
		// the whole accumulation once per page was O(n^2) transient allocation
		// inside a Wasm guest at the 100-page, 10,000-item ceiling. Marshalling
		// one page yields "[" + elements joined by "," + "]", so its length
		// minus the two brackets is exactly this page's contribution, plus one
		// byte for the comma that joins it to the pages already accumulated.
		// The total is therefore byte-identical to json.Marshal(items).
		if len(pageItems) > 0 {
			encoded, err := json.Marshal(pageItems)
			if err != nil {
				return nil, nil, err
			}
			if len(items) > 0 {
				encodedBytes++
			}
			encodedBytes += uint64(len(encoded)) - 2
		}
		items = append(items, pageItems...)
		if uint32(len(items)) > control.MaxItems || encodedBytes+2 > control.MaxBytes {
			return nil, nil, budgetExhausted("collection exceeds host ceilings")
		}
		if !hasMore {
			return items, nil, nil
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, nil, fmt.Errorf("ledger-v2: paginated response repeats a cursor")
		}
		seen[value] = struct{}{}
		cursor = next
	}
	return nil, nil, budgetExhausted("collection exceeds page ceiling")
}

func resumeGeneratedImport(ctx context.Context, command sdk.Command, ledger string, body []byte, host sdk.Host) ([]byte, error) {
	if len(command.Operations) != 2 || command.Operations[1].ID != "v2ListLogs" {
		return nil, descriptorInvalid("command %q does not declare its resume probe", command.ID)
	}
	generated, err := newGeneratedV2(host, command.Operations[1])
	if err != nil {
		return nil, err
	}
	response, err := generated.client.Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{Ledger: ledger, PageSize: ledgerclient.Int64(1)})
	if err != nil {
		return nil, generatedOperationError(command.Operations[1].ID, err)
	}
	if response == nil || response.V2LogsCursorResponse == nil {
		return nil, generatedMissing(command)
	}
	logs := response.V2LogsCursorResponse.Cursor.Data
	if len(logs) == 0 {
		return body, nil
	}
	return trimImportAfterLog(body, logs[0].ID.String())
}

func trimImportAfterLog(body []byte, lastID string) ([]byte, error) {
	for offset := 0; offset < len(body); {
		end := len(body)
		if newline := bytes.IndexByte(body[offset:], '\n'); newline >= 0 {
			end = offset + newline + 1
		}
		line := bytes.TrimSpace(body[offset:end])
		if len(line) != 0 {
			var entry struct {
				ID json.RawMessage `json:"id"`
			}
			if err := json.Unmarshal(line, &entry); err != nil {
				return nil, invalidArgument("import artifact contains malformed JSON")
			}
			id, err := normalizedLogID(entry.ID)
			if err != nil {
				return nil, invalidArgument("import artifact contains an invalid log id")
			}
			if id == lastID {
				return body[end:], nil
			}
		}
		offset = end
	}
	return nil, invalidArgument("resume log id %s is absent from the import artifact", lastID)
}

func splitImportBatches(body []byte) ([][]byte, error) {
	const maxLogsPerBatch = 100
	var batches [][]byte
	batch := make([]byte, 0, min(len(body), int(requestBulkBytes)))
	logs := 0
	flush := func() {
		if len(batch) == 0 {
			return
		}
		batches = append(batches, batch)
		batch = make([]byte, 0, min(len(body), int(requestBulkBytes)))
		logs = 0
	}
	for offset := 0; offset < len(body); {
		end := len(body)
		if newline := bytes.IndexByte(body[offset:], '\n'); newline >= 0 {
			end = offset + newline + 1
		}
		line := body[offset:end]
		if int64(len(line)) > requestBulkBytes {
			return nil, sdk.Failure{Code: string(sdk.FailureInputTooLarge), Message: "ledger-v2: one import log exceeds the request ceiling"}
		}
		if logs == maxLogsPerBatch || int64(len(batch)+len(line)) > requestBulkBytes {
			flush()
		}
		batch = append(batch, line...)
		logs++
		offset = end
	}
	flush()
	return batches, nil
}

func numscriptTransaction(ctx context.Context, request sdk.ExecuteRequest, flags map[string][]string, host sdk.Host) (components.V2PostTransaction, error) {
	source, err := readInputArtifact(ctx, host, request.Arguments[0], requestJSONBytes)
	if err != nil {
		return components.V2PostTransaction{}, err
	}
	metadata, err := parseMetadata(flags["metadata"])
	if err != nil {
		return components.V2PostTransaction{}, err
	}
	vars := make(map[string]string)
	for _, name := range []string{"account-var", "portion-var"} {
		values, err := parseMetadata(flags[name])
		if err != nil {
			return components.V2PostTransaction{}, err
		}
		for key, value := range values {
			vars[key] = value
		}
	}
	amounts, err := parseMetadata(flags["amount-var"])
	if err != nil {
		return components.V2PostTransaction{}, err
	}
	for key, value := range amounts {
		amount, asset, ok := strings.Cut(value, "/")
		if !ok || asset == "" {
			return components.V2PostTransaction{}, invalidArgument("amount variable %q must use amount/asset", key)
		}
		parsed, err := parseBigInt(amount)
		if err != nil {
			return components.V2PostTransaction{}, err
		}
		vars[key] = asset + " " + parsed.String()
	}
	timestamp, err := optionalTime(first(flags["timestamp"]))
	if err != nil {
		return components.V2PostTransaction{}, err
	}
	plain := string(source)
	return components.V2PostTransaction{Metadata: metadata, Reference: optionalString(first(flags["reference"])), Timestamp: timestamp, Script: &components.V2PostTransactionScript{Plain: &plain, Vars: vars}}, nil
}

func filterBody(flags map[string][]string) (map[string]any, error) {
	matches := make([]any, 0)
	metadata, err := parseMetadata(flags["metadata"])
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sortStrings(keys)
	for _, key := range keys {
		matches = append(matches, map[string]any{"$match": map[string]any{"metadata[" + key + "]": metadata[key]}})
	}
	for _, filter := range []struct{ flag, field string }{{"account", "account"}, {"dst", "destination"}, {"reference", "reference"}, {"src", "source"}, {"address", "address"}} {
		if value := first(flags[filter.flag]); value != "" {
			matches = append(matches, map[string]any{"$match": map[string]any{filter.field: value}})
		}
	}
	if value := first(flags["start"]); value != "" {
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return nil, invalidArgument("%q is not an RFC3339 timestamp", value)
		}
		matches = append(matches, map[string]any{"$gte": map[string]any{"timestamp": value}})
	}
	if value := first(flags["end"]); value != "" {
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return nil, invalidArgument("%q is not an RFC3339 timestamp", value)
		}
		matches = append(matches, map[string]any{"$lte": map[string]any{"timestamp": value}})
	}
	return map[string]any{"$and": matches}, nil
}

func parseSchemaData(source []byte) (components.V2SchemaData, error) {
	var document any
	if err := yaml.Unmarshal(source, &document); err != nil {
		return components.V2SchemaData{}, invalidArgument("source is not valid Ledger v2 schema JSON or YAML")
	}
	normalized, err := json.Marshal(document)
	if err != nil {
		return components.V2SchemaData{}, invalidArgument("source is not valid Ledger v2 schema JSON or YAML")
	}
	var schema components.V2SchemaData
	if err := json.Unmarshal(normalized, &schema); err != nil {
		return components.V2SchemaData{}, invalidArgument("source is not valid Ledger v2 schema JSON or YAML")
	}
	return schema, nil
}

func resolveTransactionID(ctx context.Context, command sdk.Command, ledger, value string, host sdk.Host) (*big.Int, error) {
	if !strings.HasPrefix(value, "last") {
		return parseBigInt(value)
	}

	offsetText := strings.TrimPrefix(value, "last")
	if offsetText == "-" {
		return nil, invalidArgument("%q is not last, last-N, or a non-negative integer", value)
	}
	if strings.HasPrefix(offsetText, "-") {
		offsetText = strings.TrimPrefix(offsetText, "-")
	}
	offset := new(big.Int)
	if offsetText != "" {
		parsed, ok := offset.SetString(offsetText, 10)
		if !ok || parsed.Sign() < 0 {
			return nil, invalidArgument("%q is not last, last-N, or a non-negative integer", value)
		}
	}

	var lookup *sdk.OperationPolicy
	for index := range command.Operations {
		if command.Operations[index].ID == "v2ListTransactions" {
			lookup = &command.Operations[index]
			break
		}
	}
	if lookup == nil {
		return nil, descriptorInvalid("command %q does not declare the relative transaction lookup", command.ID)
	}
	generated, err := newGeneratedV2(host, *lookup)
	if err != nil {
		return nil, fmt.Errorf("ledger-v2: configure transaction lookup: %w", err)
	}
	pageSize := int64(1)
	sort := "id:desc"
	response, err := generated.client.Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:      ledger,
		PageSize:    &pageSize,
		Sort:        &sort,
		RequestBody: map[string]any{"$and": []any{}},
	})
	if err != nil {
		return nil, fmt.Errorf("ledger-v2: v2ListTransactions: %w", err)
	}
	if response == nil || response.V2TransactionsCursorResponse == nil || len(response.V2TransactionsCursorResponse.Cursor.Data) == 0 || response.V2TransactionsCursorResponse.Cursor.Data[0].ID == nil {
		return nil, invalidArgument("no transaction found for %q", value)
	}
	resolved := new(big.Int).Sub(response.V2TransactionsCursorResponse.Cursor.Data[0].ID, offset)
	if resolved.Sign() < 0 {
		return nil, invalidArgument("%q resolves before transaction zero", value)
	}
	return resolved, nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func optionalInt64(value string) *int64 {
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}
func optionalBool(value string) (*bool, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return nil, invalidArgument("%q is not a boolean", value)
	}
	return &parsed, nil
}
func optionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, invalidArgument("%q is not an RFC3339 timestamp", value)
	}
	return &parsed, nil
}
func parseBigInt(value string) (*big.Int, error) {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok || parsed.Sign() < 0 {
		return nil, invalidArgument("%q is not a non-negative integer", value)
	}
	return parsed, nil
}

func emitGeneratedJSON(host sdk.Host, command sdk.Command, shape sdk.ResultShape, value any, page *sdk.PageInfo) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("ledger-v2: encode generated result: %w", err)
	}
	return emitBytes(host, command.ID, shape, mediaTypeJSON, data, page)
}
func emitGeneratedEmpty(host sdk.Host, command sdk.Command, err error) error {
	if err != nil {
		return generatedError(command, err)
	}
	return emitBytes(host, command.ID, sdk.ResultObject, mediaTypeJSON, []byte(`{}`), nil)
}
func generatedError(command sdk.Command, err error) error {
	return generatedOperationError(command.Operations[0].ID, err)
}
func generatedOperationError(operationID string, err error) error {
	return fmt.Errorf("ledger-v2: %s: %w", operationID, err)
}
func generatedMissing(command sdk.Command) error {
	return fmt.Errorf("ledger-v2: %s returned no result", command.Operations[0].ID)
}
