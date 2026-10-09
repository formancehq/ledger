package shared

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/fctl/pkg/pluginsdk"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// write translates a normalized plugin command into native Apply requests.
// Apply owns signing and response verification. No mutation is retried here,
// including when rendering its committed result fails.
func (e *Executor) write(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, bool, error) {
	path := strings.Join(req.CommandPath, "/")
	if !isWriteCommand(path) {
		return pluginsdk.ExecuteResponse{}, false, nil
	}
	e.Result, e.LastApplyResponse = nil, nil
	if err := ctx.Err(); err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	if e.Apply == nil {
		return pluginsdk.ExecuteResponse{}, true, errors.New("ledger mutation requires an Apply executor")
	}
	if path == "ledger/bulk" {
		response, err := e.writeBulk(ctx, req)

		return response, true, err
	}

	requests, err := e.writeRequests(path, req)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	if err := ctx.Err(); err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	result, err := e.Apply(ctx, req.Flags["idempotency-key"], requests...)
	e.LastApplyResponse = result
	if err != nil {
		return pluginsdk.ExecuteResponse{}, true, err
	}
	response, err := e.writeResult(path, requests, result)

	return response, true, err
}

func isWriteCommand(path string) bool {
	switch path {
	case "ledger/create", "ledger/delete", "ledger/metadata/set", "ledger/metadata/delete",
		"ledger/accounts/metadata/set", "ledger/accounts/metadata/delete",
		"ledger/transactions/create", "ledger/transactions/revert",
		"ledger/transactions/metadata/set", "ledger/transactions/metadata/delete",
		"ledger/indexes/create", "ledger/indexes/delete", "ledger/bulk":
		return true
	default:
		return false
	}
}

func (e *Executor) writeRequests(path string, req pluginsdk.ExecuteRequest) ([]*servicepb.Request, error) {
	ledger := req.Flags["ledger"]
	if (path == "ledger/create" || path == "ledger/delete") && len(req.Args) > 0 {
		ledger = req.Args[0]
	}
	if ledger == "" {
		return nil, errors.New("ledger name is required")
	}

	var request *servicepb.Request
	switch path {
	case "ledger/create":
		if len(e.CreateLedgerRequests) > 0 {
			// Native creation can attach initial indexes to the same atomic
			// proposal and supports mirror IAM fields absent from the REST DTO.
			if e.CreateLedgerRequests[0].GetCreateLedger().GetName() != ledger {
				return nil, errors.New("native creation batch targets a different ledger")
			}

			return e.CreateLedgerRequests, nil
		}
		create, err := decodeWriteLedger(ledger, req.Body)
		if err != nil {
			return nil, err
		}
		request = &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: create}}
	case "ledger/delete":
		request = &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: ledger}}}
	case "ledger/metadata/set":
		metadata, err := decodeWriteMetadata(req.Body)
		if err != nil {
			return nil, err
		}
		request = &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: ledger, Metadata: metadata}}}
	case "ledger/metadata/delete":
		if len(req.Args) != 1 {
			return nil, errors.New("metadata key is required")
		}
		request = &servicepb.Request{Type: &servicepb.Request_DeleteLedgerMetadata{DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{Ledger: ledger, Key: req.Args[0]}}}
	case "ledger/accounts/metadata/set", "ledger/accounts/metadata/delete", "ledger/transactions/metadata/set", "ledger/transactions/metadata/delete":
		action, err := decodeWriteMetadataAction(path, req)
		if err != nil {
			return nil, err
		}
		request = writeActionRequest(ledger, action, nil)
	case "ledger/transactions/create":
		payload := &servicepb.CreateTransactionPayload{}
		if err := json.Unmarshal(req.Body, payload); err != nil {
			return nil, fmt.Errorf("invalid transaction body: %w", err)
		}
		request = writeActionRequest(ledger, &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}, nil)
	case "ledger/transactions/revert":
		payload, err := decodeWriteRevert(req)
		if err != nil {
			return nil, err
		}
		request = writeActionRequest(ledger, &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{RevertTransaction: payload}}, nil)
	case "ledger/indexes/create":
		var body struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, fmt.Errorf("invalid index body: %w", err)
		}
		if body.ID == "" {
			return nil, errors.New("index id is required")
		}
		id, err := indexes.ParseCanonical(body.ID)
		if err != nil {
			return nil, err
		}
		request = &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledger, Id: id}}}
	case "ledger/indexes/delete":
		if len(req.Args) != 1 {
			return nil, errors.New("index id is required")
		}
		id, err := indexes.ParseCanonical(req.Args[0])
		if err != nil {
			return nil, err
		}
		request = &servicepb.Request{Type: &servicepb.Request_DropIndex{DropIndex: &servicepb.DropIndexRequest{Ledger: ledger, Id: id}}}
	default:
		return nil, fmt.Errorf("unsupported mutation %q", path)
	}

	return []*servicepb.Request{request}, nil
}

func writeActionRequest(ledger string, action *servicepb.LedgerAction, skippable []commonpb.ErrorReason) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: ledger, Action: action, SkippableReasons: skippable}}}
}

func decodeWriteMetadata(body stdjson.RawMessage) (map[string]*commonpb.MetadataValue, error) {
	var input map[string]any
	if err := json.UnmarshalUseNumber(body, &input); err != nil {
		return nil, fmt.Errorf("invalid metadata body: %w", err)
	}
	metadata, err := commonpb.MetadataFromAnyMap(input)
	if err != nil {
		return nil, fmt.Errorf("invalid metadata: %w", err)
	}

	return metadata, nil
}

func decodeWriteMetadataAction(path string, req pluginsdk.ExecuteRequest) (*servicepb.LedgerAction, error) {
	args := 1
	if strings.HasSuffix(path, "/delete") {
		args++
	}
	if len(req.Args) != args {
		return nil, errors.New("metadata owner and key arguments are required")
	}
	var target *commonpb.Target
	if strings.HasPrefix(path, "ledger/accounts/") {
		target = &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: req.Args[0]}}}
	} else {
		id, err := strconv.ParseUint(req.Args[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid transaction id: %w", err)
		}
		target = &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}
	}
	if strings.HasSuffix(path, "/delete") {
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{Target: target, Key: req.Args[1]}}}, nil
	}
	metadata, err := decodeWriteMetadata(req.Body)
	if err != nil {
		return nil, err
	}

	return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{Target: target, Metadata: metadata}}}, nil
}

func decodeWriteRevert(req pluginsdk.ExecuteRequest) (*servicepb.RevertTransactionPayload, error) {
	if len(req.Args) != 1 {
		return nil, errors.New("transaction id is required")
	}
	id, err := strconv.ParseUint(req.Args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction id: %w", err)
	}
	payload := &servicepb.RevertTransactionPayload{TransactionId: id}
	if len(req.Body) == 0 {
		return payload, nil
	}
	// Match the unitary HTTP handler: its path owns the transaction id, and
	// optional fields with another JSON type are ignored. Bulk has its own
	// stricter, custom BulkElement decoder with data.id.
	var body map[string]any
	if err := json.UnmarshalUseNumber(req.Body, &body); err != nil {
		return nil, fmt.Errorf("invalid revert body: %w", err)
	}
	if values, ok := body["metadata"].(map[string]any); ok {
		payload.Metadata, err = commonpb.MetadataFromAnyMap(values)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata: %w", err)
		}
	}
	payload.Force, _ = body["force"].(bool)
	payload.AtEffectiveDate, _ = body["atEffectiveDate"].(bool)

	return payload, nil
}

// These DTOs mirror the camelCase HTTP creation contract. Generated protobuf
// tags cannot decode this shape (notably initialSchema and mirrorSource), while
// metadata, account segment types and rewrite rules reuse their native decoders.
type writeLedgerBody struct {
	Metadata               *commonpb.MetadataMap           `json:"metadata"`
	Mode                   string                          `json:"mode"`
	MirrorSource           *writeMirrorBody                `json:"mirrorSource"`
	DefaultEnforcementMode string                          `json:"defaultEnforcementMode"`
	InitialSchema          []writeSchemaBody               `json:"initialSchema"`
	AccountTypes           map[string]writeAccountTypeBody `json:"accountTypes"`
}

type writeSchemaBody struct {
	TargetType string `json:"targetType"`
	Key        string `json:"key"`
	Type       string `json:"type"`
}

type writeAccountTypeBody struct {
	Name         string                               `json:"name"`
	Pattern      string                               `json:"pattern"`
	Persistence  string                               `json:"persistence"`
	SegmentTypes map[string]*commonpb.SegmentTypeJSON `json:"segmentTypes"`
}

type writeMirrorBody struct {
	LedgerName          string               `json:"ledgerName"`
	Type                string               `json:"type"`
	BaseURL             string               `json:"baseUrl"`
	OAuth2ClientID      string               `json:"oauth2ClientId"`
	OAuth2ClientSecret  string               `json:"oauth2ClientSecret"`
	OAuth2TokenEndpoint string               `json:"oauth2TokenEndpoint"`
	OAuth2Scopes        []string             `json:"oauth2Scopes"`
	DSN                 string               `json:"dsn"`
	BatchSize           uint32               `json:"batchSize"`
	RewriteRules        []stdjson.RawMessage `json:"rewriteRules"`
}

func decodeWriteLedger(name string, data stdjson.RawMessage) (*servicepb.CreateLedgerRequest, error) {
	var body writeLedgerBody
	if len(data) > 0 {
		if err := json.Unmarshal(data, &body); err != nil {
			return nil, fmt.Errorf("invalid ledger body: %w", err)
		}
	}
	request := &servicepb.CreateLedgerRequest{Name: name, Metadata: body.Metadata.GetValues()}
	if body.Mode == "MIRROR" {
		request.Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
		if body.MirrorSource != nil {
			mirror, err := decodeWriteMirror(body.MirrorSource)
			if err != nil {
				return nil, err
			}
			request.MirrorSource = mirror
		}
	}
	if body.DefaultEnforcementMode != "" {
		switch body.DefaultEnforcementMode {
		case "STRICT", "strict":
			request.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		case "AUDIT", "audit":
			request.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
		default:
			return nil, errors.New("invalid enforcement mode: must be STRICT or AUDIT")
		}
	}
	for i, field := range body.InitialSchema {
		target, err := commonpb.ParseTargetType(field.TargetType)
		if err != nil {
			return nil, fmt.Errorf("initialSchema[%d]: %w", i, err)
		}
		kind, err := commonpb.ParseMetadataType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("initialSchema[%d]: %w", i, err)
		}
		request.InitialSchema = append(request.InitialSchema, &commonpb.SetMetadataFieldTypeCommand{TargetType: target, Key: field.Key, Type: kind})
	}
	if len(body.AccountTypes) > 0 {
		request.AccountTypes = make(map[string]*commonpb.AccountType, len(body.AccountTypes))
		for key, value := range body.AccountTypes {
			accountType, err := decodeWriteAccountType(value)
			if err != nil {
				return nil, fmt.Errorf("accountTypes[%q]: %w", key, err)
			}
			request.AccountTypes[key] = accountType
		}
	}

	return request, nil
}

func decodeWriteAccountType(body writeAccountTypeBody) (*commonpb.AccountType, error) {
	persistence, err := commonpb.ParsePersistence(body.Persistence)
	if err != nil {
		return nil, err
	}
	accountType := &commonpb.AccountType{Name: body.Name, Pattern: body.Pattern, Persistence: persistence}
	if len(body.SegmentTypes) > 0 {
		accountType.SegmentTypes = make(map[string]*commonpb.SegmentType, len(body.SegmentTypes))
		for key, value := range body.SegmentTypes {
			segment, err := commonpb.SegmentTypeFromJSON(value)
			if err != nil {
				return nil, fmt.Errorf("segmentTypes[%q]: %w", key, err)
			}
			accountType.SegmentTypes[key] = segment
		}
	}

	return accountType, nil
}

func decodeWriteMirror(body *writeMirrorBody) (*commonpb.MirrorSourceConfig, error) {
	config := &commonpb.MirrorSourceConfig{LedgerName: body.LedgerName, BatchSize: body.BatchSize}
	for i, data := range body.RewriteRules {
		if len(data) == 0 || string(data) == "null" {
			return nil, fmt.Errorf("rewriteRules[%d]: rule must not be empty", i)
		}
		rule := &commonpb.MirrorRewriteRule{}
		if err := protojson.Unmarshal(data, rule); err != nil {
			return nil, fmt.Errorf("rewriteRules[%d]: %w", i, err)
		}
		config.RewriteRules = append(config.RewriteRules, rule)
	}
	switch body.Type {
	case "http", "":
		http := &commonpb.HttpMirrorSourceConfig{BaseUrl: body.BaseURL}
		if body.OAuth2ClientID != "" || body.OAuth2TokenEndpoint != "" {
			http.Oauth2ClientCredentials = &commonpb.OAuth2ClientCredentials{ClientId: body.OAuth2ClientID, ClientSecret: body.OAuth2ClientSecret, TokenEndpoint: body.OAuth2TokenEndpoint, Scopes: body.OAuth2Scopes}
		}
		config.Type = &commonpb.MirrorSourceConfig_Http{Http: http}
	case "postgres":
		config.Type = &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: body.DSN}}
	default:
		return nil, fmt.Errorf("unsupported mirror source type: %q", body.Type)
	}

	return config, nil
}

func (e *Executor) writeResult(path string, requests []*servicepb.Request, response *servicepb.ApplyResponse) (pluginsdk.ExecuteResponse, error) {
	e.Result = nil
	if len(response.GetLogs()) != len(requests) {
		return pluginsdk.ExecuteResponse{}, fmt.Errorf("apply returned %d logs for %d requests; mutation may have committed", len(response.GetLogs()), len(requests))
	}
	log := response.GetLogs()[0]
	switch path {
	case "ledger/create":
		created := log.GetPayload().GetCreateLedger()
		if created == nil {
			return pluginsdk.ExecuteResponse{}, errors.New("apply returned no created ledger payload; mutation may have committed")
		}
		info := created.ToLedgerInfo()
		info.Metadata = created.GetMetadata()
		e.Result = info

		return marshalWriteResponse(info)
	case "ledger/transactions/create":
		created := log.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()
		if created == nil {
			return pluginsdk.ExecuteResponse{}, errors.New("apply returned no created transaction payload; mutation may have committed")
		}
		e.Result = created

		return marshalWriteResponse(created)
	case "ledger/transactions/revert":
		reverted := log.GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction()
		if reverted == nil {
			return pluginsdk.ExecuteResponse{}, errors.New("apply returned no reverted transaction payload; mutation may have committed")
		}
		e.Result = reverted

		return marshalWriteResponse(reverted)
	case "ledger/indexes/create":
		created := log.GetPayload().GetApply().GetLog().GetData().GetCreateIndex()
		if created == nil {
			return pluginsdk.ExecuteResponse{}, errors.New("apply returned no created index payload; mutation may have committed")
		}
		e.Result = created

		return marshalWriteResponse(struct {
			ID string `json:"id"`
		}{ID: indexes.Canonical(requests[0].GetCreateIndex().GetId())})
	case "ledger/metadata/set":
		e.Result = log.GetPayload().GetSavedLedgerMetadata()
	case "ledger/metadata/delete":
		e.Result = log.GetPayload().GetDeletedLedgerMetadata()
	case "ledger/accounts/metadata/set", "ledger/transactions/metadata/set":
		e.Result = log.GetPayload().GetApply().GetLog().GetData().GetSavedMetadata()
	case "ledger/accounts/metadata/delete", "ledger/transactions/metadata/delete":
		e.Result = log.GetPayload().GetApply().GetLog().GetData().GetDeletedMetadata()
	case "ledger/delete":
		e.Result = log.GetPayload().GetDeleteLedger()
	case "ledger/indexes/delete":
		e.Result = log.GetPayload().GetApply().GetLog().GetData().GetDropIndex()
	}
	if !validWriteResult(e.Result) {
		e.Result = nil

		return pluginsdk.ExecuteResponse{}, fmt.Errorf("apply returned no %s payload; mutation may have committed", path)
	}
	// These HTTP endpoints return 204; native renderers consume Result instead.
	return pluginsdk.ExecuteResponse{}, nil
}

func validWriteResult(value any) bool {
	message, ok := value.(proto.Message)

	return ok && message.ProtoReflect().IsValid()
}

func marshalWriteResponse(value any) (pluginsdk.ExecuteResponse, error) {
	data, err := cmdutil.MarshalJSON(value)
	if err != nil {
		return pluginsdk.ExecuteResponse{}, err
	}
	envelope, err := stdjson.Marshal(struct {
		Data stdjson.RawMessage `json:"data"`
	}{Data: data})

	return pluginsdk.ExecuteResponse{Data: envelope}, err
}

type writeBulkResult struct {
	ErrorCode        string             `json:"errorCode,omitempty"`
	ErrorDescription string             `json:"errorDescription,omitempty"`
	Data             stdjson.RawMessage `json:"data,omitempty"`
	ResponseType     string             `json:"responseType"`
	LogID            uint64             `json:"logID"`
}

func (e *Executor) writeBulk(ctx context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
	var elements []*servicepb.BulkElement
	if err := json.Unmarshal(req.Body, &elements); err != nil {
		return pluginsdk.ExecuteResponse{}, fmt.Errorf("invalid bulk body: %w", err)
	}
	requests := make([]*servicepb.Request, len(elements))
	for i, element := range elements {
		if element == nil || element.Action == nil || servicepb.GetLedgerActionType(element.Action) == "" {
			return pluginsdk.ExecuteResponse{}, fmt.Errorf("bulk element %d has no action", i)
		}
		requests[i] = writeActionRequest(req.Flags["ledger"], element.Action, element.SkippableReasons)
	}
	atomic, err := strconv.ParseBool(req.Flags["atomic"])
	if err != nil {
		return pluginsdk.ExecuteResponse{}, fmt.Errorf("invalid atomic flag: %w", err)
	}
	continued, err := strconv.ParseBool(req.Flags["continue-on-failure"])
	if err != nil {
		return pluginsdk.ExecuteResponse{}, fmt.Errorf("invalid continue-on-failure flag: %w", err)
	}
	logs := make([]*commonpb.Log, len(elements))
	failures := make([]error, len(elements))
	var failure error
	if len(elements) > 0 && atomic {
		if err := ctx.Err(); err != nil {
			return pluginsdk.ExecuteResponse{}, err
		}
		response, applyErr := e.Apply(ctx, req.Flags["idempotency-key"], requests...)
		e.LastApplyResponse = response
		if applyErr == nil && len(response.GetLogs()) != len(requests) {
			applyErr = fmt.Errorf("apply returned %d logs for %d bulk requests; batch may have committed", len(response.GetLogs()), len(requests))
		}
		failure = applyErr
		for i := range elements {
			if applyErr != nil {
				failures[i] = applyErr

				continue
			}
			logs[i] = response.GetLogs()[i]
		}
	} else {
		for i, element := range elements {
			if failure != nil && !continued {
				failures[i] = context.Canceled

				continue
			}
			if err := ctx.Err(); err != nil {
				failures[i] = err
				if failure == nil {
					failure = err
				}

				continue
			}
			response, applyErr := e.Apply(ctx, element.IdempotencyKey, requests[i])
			e.LastApplyResponse = response
			if applyErr == nil && len(response.GetLogs()) != 1 {
				applyErr = fmt.Errorf("apply returned %d logs for one bulk request; mutation may have committed", len(response.GetLogs()))
			}
			if applyErr != nil {
				failures[i] = applyErr
				if failure == nil {
					failure = fmt.Errorf("bulk element %d: %w", i, applyErr)
				}

				continue
			}
			logs[i] = response.GetLogs()[0]
		}
	}
	// Match HTTP: encoding happens after execution. A display failure cannot
	// change which subsequent operations the caller's bulk policy executes.
	results := make([]writeBulkResult, len(elements))
	for i, element := range elements {
		if failures[i] != nil {
			results[i] = writeBulkFailure(failures[i])

			continue
		}
		result, err := writeBulkSuccess(element, logs[i])
		results[i] = result
		if err != nil {
			failure = errors.Join(failure, fmt.Errorf("bulk element %d: %w", i, err))
		}
	}
	data, encodeErr := stdjson.Marshal(struct {
		Data []writeBulkResult `json:"data,omitempty"`
	}{Data: results})
	e.Result = stdjson.RawMessage(data)

	return pluginsdk.ExecuteResponse{Data: data}, errors.Join(failure, encodeErr)
}

func writeBulkSuccess(element *servicepb.BulkElement, log *commonpb.Log) (writeBulkResult, error) {
	ledgerLog := log.GetPayload().GetApply().GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		err := errors.New("apply returned no ledger log payload; mutation may have committed")

		return writeBulkFailure(err), err
	}
	result := writeBulkResult{ResponseType: servicepb.GetLedgerActionType(element.Action), LogID: ledgerLog.GetId()}
	var value any
	switch payload := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		value = payload.CreatedTransaction.GetTransaction()
	case *commonpb.LedgerLogPayload_OrderSkipped:
		value = struct {
			Skipped bool              `json:"skipped"`
			Reason  string            `json:"reason"`
			Context map[string]string `json:"context,omitempty"`
		}{Skipped: true, Reason: domain.ReasonString(payload.OrderSkipped.GetReason()), Context: payload.OrderSkipped.GetContext()}
	}
	if value != nil {
		data, err := cmdutil.MarshalJSON(value)
		if err != nil {
			return writeBulkFailure(err), err
		}
		result.Data = data
	}

	return result, nil
}

func writeBulkFailure(err error) writeBulkResult {
	result := writeBulkResult{ResponseType: "ERROR", ErrorCode: "ERROR", ErrorDescription: "internal server error"}
	decoded := err
	if remote := grpcerr.Decode(err); remote != nil {
		decoded = remote
	}
	if _, invalid := apierr.InvalidWire(decoded); invalid {
		result.ErrorCode = "INTERNAL_ERROR"

		return result
	}
	if descriptor, ok := apierr.Describe(decoded); ok {
		result.ErrorCode = descriptor.Reason
		result.ErrorDescription = descriptor.Message
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result.ErrorDescription = err.Error()
	}

	return result
}
