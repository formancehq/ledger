package ledgerv3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	ledgerconfig "github.com/formancehq/ledger/v3/plugins/fctl/ledger-v3/internal/ledgerconfig"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

func executeV3Ledgers(ctx context.Context, _ sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch command.ID {
	case "ledger.v3.ledgers.configuration", "ledger.v3.ledgers.configuration.export":
		configuration, err := fetchV3LedgerConfiguration(ctx, decoded.text(argLedger), command, host)
		if err != nil {
			return true, err
		}
		return true, emitJSON(host, command.Operations[0].ID, sdk.ResultObject, configuration, nil)
	case "ledger.v3.ledgers.configuration.apply":
		content, err := readArtifact(ctx, host, decoded.text(flagConfiguration), maxConfigurationBytes)
		if err != nil {
			return true, err
		}
		var desired ledgerconfig.EditableConfig
		if json.Valid(content) {
			err = json.Unmarshal(content, &desired)
		} else {
			err = yaml.Unmarshal(content, &desired)
		}
		if err != nil {
			return true, invalidArgument("invalid configuration document")
		}
		current, err := fetchV3LedgerConfiguration(ctx, decoded.text(argLedger), command, host)
		if err != nil {
			return true, err
		}
		actions, err := ledgerconfig.ComputeDiff(decoded.text(argLedger), current, &desired)
		if err != nil {
			return true, invalidArgument("invalid desired configuration")
		}
		requests := make([]*servicepb.Request, 0, len(actions))
		for _, action := range actions {
			requests = append(requests, action.Request)
		}
		if len(requests) == 0 {
			return true, emitEmpty(host, opApplyConfiguration.id)
		}
		if decoded.boolean(flagDryRun) {
			type plannedAction struct {
				Section     string `json:"section"`
				Operation   string `json:"operation"`
				Description string `json:"description,omitempty"`
			}
			plan := struct {
				Actions []plannedAction `json:"actions"`
			}{Actions: make([]plannedAction, 0, len(actions))}
			for _, action := range actions {
				plan.Actions = append(plan.Actions, plannedAction{Section: action.Section, Operation: action.Operation, Description: action.Description})
			}
			return true, emitJSON(host, opApplyConfiguration.id, sdk.ResultObject, plan, nil)
		}
		response, err := applyV3(ctx, host, command, opApplyConfiguration.id, decoded.text(flagIdempotencyKey), requests...)
		if err != nil {
			return true, err
		}
		return true, emitProto(host, opApplyConfiguration.id, response)
	case "ledger.v3.ledgers.create":
		schema, err := parseInitialSchema(decoded.list(flagMetadataType))
		if err != nil {
			return true, err
		}
		ledgerMode, mirrorSource, err := parseMirrorSource(ctx, host, decoded, decoded.text(argName))
		if err != nil {
			return true, err
		}
		mode := commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		if decoded.text(flagEnforcementMode) == "audit" {
			mode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
		}
		response, err := applyV3(ctx, host, command, opApplyCreateLedger.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: decoded.text(argName), InitialSchema: schema, DefaultEnforcementMode: mode, Mode: ledgerMode, MirrorSource: mirrorSource}}})
		if err != nil {
			return true, err
		}
		if len(response.GetLogs()) != 0 && response.GetLogs()[0].GetPayload().GetCreateLedger() != nil {
			return true, emitProto(host, opApplyCreateLedger.id, response.GetLogs()[0].GetPayload().GetCreateLedger().ToLedgerInfo())
		}
		return true, v3Failure("create ledger returned no ledger")
	case "ledger.v3.ledgers.delete":
		response, err := applyV3(ctx, host, command, opApplyDeleteLedger.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: decoded.text(argName)}}})
		if err != nil {
			return true, err
		}
		if len(response.GetLogs()) != 0 && response.GetLogs()[0].GetPayload().GetDeleteLedger() != nil {
			return true, emitProto(host, opApplyDeleteLedger.id, response.GetLogs()[0].GetPayload().GetDeleteLedger())
		}
		return true, v3Failure("delete ledger returned no ledger")
	case "ledger.v3.ledgers.delete-metadata":
		return true, applyLedgerMutation(ctx, host, command, decoded, opApplyDeleteLedgerMetadata.id, &servicepb.Request{Type: &servicepb.Request_DeleteLedgerMetadata{DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{Ledger: decoded.text(argLedger), Key: decoded.text(argKey)}}})
	case "ledger.v3.ledgers.set-metadata":
		metadata, err := parseMetadata(decoded.list(flagMetadata))
		if err != nil {
			return true, err
		}
		return true, applyLedgerMutation(ctx, host, command, decoded, opApplySaveLedgerMetadata.id, &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: decoded.text(argLedger), Metadata: metadata}}})
	case "ledger.v3.ledgers.set-metadata-type", "ledger.v3.ledgers.remove-metadata-type":
		target, err := commonpb.ParseTargetType(decoded.text(flagTargetType))
		if err != nil {
			return true, invalidArgument("invalid target type")
		}
		if command.ID == "ledger.v3.ledgers.remove-metadata-type" {
			return true, applyLedgerMutation(ctx, host, command, decoded, opApplyRemoveMetadataType.id, &servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{Ledger: decoded.text(argLedger), TargetType: target, Key: decoded.text(argKey)}}})
		}
		metadataType, err := commonpb.ParseMetadataType(decoded.text(flagMetadataType))
		if err != nil {
			return true, invalidArgument("invalid metadata type")
		}
		return true, applyLedgerMutation(ctx, host, command, decoded, opApplySetMetadataFieldType.id, &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{Ledger: decoded.text(argLedger), TargetType: target, Key: decoded.text(argKey), Type: metadataType}}})
	default:
		return false, nil
	}
}

func parseMirrorSource(ctx context.Context, host sdk.Host, decoded input, ledgerName string) (commonpb.LedgerMode, *commonpb.MirrorSourceConfig, error) {
	mirrorFlags := []string{flagMirrorSourceType, flagMirrorLedgerName, flagMirrorBaseURL, flagMirrorOAuth2ClientID, flagMirrorOAuth2ClientSecret, flagMirrorOAuth2TokenEndpoint, flagMirrorOAuth2Scopes, flagMirrorDSN, flagMirrorAWSRegion, flagMirrorAWSRoleARN, flagMirrorBatchSize, flagMirrorRewriteFile, flagMirrorRewriteRule}
	hasMirrorFlags := false
	for _, name := range mirrorFlags {
		if decoded.present[name] {
			hasMirrorFlags = true
			break
		}
	}
	mode := decoded.text(flagMode)
	if hasMirrorFlags && !decoded.present[flagMode] {
		mode = "mirror"
	}
	if mode == "normal" {
		if hasMirrorFlags {
			return 0, nil, invalidArgument("mirror flags require mode mirror")
		}
		return commonpb.LedgerMode_LEDGER_MODE_NORMAL, nil, nil
	}
	if mode != "mirror" {
		return 0, nil, invalidArgument("flag %q expects normal or mirror", flagMode)
	}
	var batchSize uint64
	if raw := decoded.text(flagMirrorBatchSize); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return 0, nil, invalidArgument("flag %q expects an unsigned 32-bit integer", flagMirrorBatchSize)
		}
		batchSize = parsed
	}
	sourceLedger := decoded.text(flagMirrorLedgerName)
	if sourceLedger == "" {
		sourceLedger = ledgerName
	}
	config := &commonpb.MirrorSourceConfig{LedgerName: sourceLedger, BatchSize: uint32(batchSize)}
	if handle := decoded.text(flagMirrorRewriteFile); handle != "" {
		content, err := readArtifact(ctx, host, handle, maxConfigurationBytes)
		if err != nil {
			return 0, nil, err
		}
		if !json.Valid(content) {
			var value any
			if err := yaml.Unmarshal(content, &value); err != nil {
				return 0, nil, invalidArgument("flag %q contains an invalid mirror rewrite document", flagMirrorRewriteFile)
			}
			content, err = json.Marshal(value)
			if err != nil {
				return 0, nil, invalidArgument("flag %q contains an invalid mirror rewrite document", flagMirrorRewriteFile)
			}
		}
		var encodedRules []json.RawMessage
		if err := json.Unmarshal(content, &encodedRules); err != nil {
			return 0, nil, invalidArgument("flag %q expects a list of mirror rewrite rules", flagMirrorRewriteFile)
		}
		for _, encodedRule := range encodedRules {
			rule := &commonpb.MirrorRewriteRule{}
			if err := protojson.Unmarshal(encodedRule, rule); err != nil || rule.GetScope() == nil {
				return 0, nil, invalidArgument("flag %q contains an invalid mirror rewrite rule", flagMirrorRewriteFile)
			}
			config.RewriteRules = append(config.RewriteRules, rule)
		}
	}
	for _, encodedRule := range decoded.list(flagMirrorRewriteRule) {
		rule := &commonpb.MirrorRewriteRule{}
		if err := protojson.Unmarshal([]byte(encodedRule), rule); err != nil || rule.GetScope() == nil {
			return 0, nil, invalidArgument("flag %q contains an invalid mirror rewrite rule", flagMirrorRewriteRule)
		}
		config.RewriteRules = append(config.RewriteRules, rule)
	}
	switch decoded.text(flagMirrorSourceType) {
	case "http":
		baseURL := decoded.text(flagMirrorBaseURL)
		if baseURL == "" {
			return 0, nil, invalidArgument("flag %q is required for an HTTP mirror source", flagMirrorBaseURL)
		}
		httpSource := &commonpb.HttpMirrorSourceConfig{BaseUrl: baseURL}
		if decoded.text(flagMirrorOAuth2ClientID) != "" || decoded.text(flagMirrorOAuth2ClientSecret) != "" || decoded.text(flagMirrorOAuth2TokenEndpoint) != "" || len(decoded.list(flagMirrorOAuth2Scopes)) != 0 {
			httpSource.Oauth2ClientCredentials = &commonpb.OAuth2ClientCredentials{ClientId: decoded.text(flagMirrorOAuth2ClientID), ClientSecret: decoded.text(flagMirrorOAuth2ClientSecret), TokenEndpoint: decoded.text(flagMirrorOAuth2TokenEndpoint), Scopes: decoded.list(flagMirrorOAuth2Scopes)}
		}
		config.Type = &commonpb.MirrorSourceConfig_Http{Http: httpSource}
	case "postgres":
		dsn := decoded.text(flagMirrorDSN)
		if dsn == "" {
			return 0, nil, invalidArgument("flag %q is required for a postgres mirror source", flagMirrorDSN)
		}
		postgres := &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}
		region, role := decoded.text(flagMirrorAWSRegion), decoded.text(flagMirrorAWSRoleARN)
		if decoded.has(flagMirrorAWSRegion) && region == "" {
			return 0, nil, invalidArgument("flag %q cannot be empty", flagMirrorAWSRegion)
		}
		if decoded.has(flagMirrorAWSRoleARN) && role == "" {
			return 0, nil, invalidArgument("flag %q cannot be empty", flagMirrorAWSRoleARN)
		}
		if role != "" && region == "" {
			return 0, nil, invalidArgument("flag %q requires flag %q", flagMirrorAWSRoleARN, flagMirrorAWSRegion)
		}
		if region != "" {
			postgres.AwsIamAuth = &commonpb.PostgresAwsIamAuth{Region: region, AssumeRoleArn: role}
		}
		config.Type = &commonpb.MirrorSourceConfig_Postgres{Postgres: postgres}
	default:
		return 0, nil, invalidArgument("flag %q expects http or postgres", flagMirrorSourceType)
	}
	return commonpb.LedgerMode_LEDGER_MODE_MIRROR, config, nil
}

func applyLedgerMutation(ctx context.Context, host sdk.Host, command sdk.Command, decoded input, operation string, request *servicepb.Request) error {
	_, err := applyV3(ctx, host, command, operation, decoded.text(flagIdempotencyKey), request)
	if err != nil {
		return err
	}
	return emitEmpty(host, operation)
}

func fetchV3LedgerConfiguration(ctx context.Context, ledger string, command sdk.Command, host sdk.Host) (*ledgerconfig.EditableConfig, error) {
	info, err := unaryV3(ctx, host, command, opGetLedger.id, &servicepb.GetLedgerRequest{Ledger: ledger}, &commonpb.LedgerInfo{})
	if err != nil {
		return nil, v3Failure("get ledger configuration: %v", err)
	}
	indexStream, err := streamV3(ctx, host, command, opListIndexes.id, &servicepb.ListIndexesRequest{Scope: servicepb.ListIndexesRequest_SCOPE_LEDGER, Ledger: ledger}, func() *commonpb.Index { return &commonpb.Index{} })
	if err != nil {
		return nil, v3Failure("list configuration indexes: %v", err)
	}
	var indexes []*commonpb.Index
	for {
		item := indexStream.newItem()
		recvErr := indexStream.stream.RecvInto(item)
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, v3Failure("receive configuration indexes: %v", recvErr)
		}
		indexes = append(indexes, item)
	}
	queries, err := unaryV3(ctx, host, command, opListPreparedQueries.id, &servicepb.ListPreparedQueriesRequest{Ledger: ledger}, &servicepb.ListPreparedQueriesResponse{})
	if err != nil {
		return nil, v3Failure("list configuration queries: %v", err)
	}
	numscripts, err := fetchAllV3Numscripts(ctx, ledger, command, host)
	if err != nil {
		return nil, err
	}
	configuration := ledgerconfig.ConfigFromProto(info, indexes, queries.GetQueries(), numscripts)
	if err := enforceV3ConfigurationJSONBudget(configuration); err != nil {
		return nil, err
	}
	return configuration, nil
}

func fetchAllV3Numscripts(ctx context.Context, ledger string, command sdk.Command, host sdk.Host) ([]*commonpb.NumscriptInfo, error) {
	cursor := ""
	seen := map[string]struct{}{}
	budget := newV3NumscriptJSONBudget()
	var result []*commonpb.NumscriptInfo
	for page := uint32(0); page < sdk.DefaultAllPagesMaxPages; page++ {
		stream, err := streamV3(ctx, host, command, opListNumscripts.id, &servicepb.ListNumscriptsRequest{Ledger: ledger, Options: &commonpb.ListOptions{PageSize: 100, Cursor: cursor}}, func() *commonpb.NumscriptInfo { return &commonpb.NumscriptInfo{} })
		if err != nil {
			return nil, v3Failure("list configuration numscripts: %v", err)
		}
		for {
			item := stream.newItem()
			recvErr := stream.stream.RecvInto(item)
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				return nil, v3Failure("receive configuration numscripts: %v", recvErr)
			}
			if len(result) >= int(sdk.DefaultAllPagesMaxItems) {
				return nil, v3BudgetExceeded("configuration numscript collection exceeded the item limit")
			}
			if err := budget.add(item); err != nil {
				return nil, err
			}
			result = append(result, item)
		}
		next, ok := stream.stream.Continuation()
		if !ok {
			return result, nil
		}
		if _, duplicate := seen[next]; duplicate {
			return nil, v3Failure("configuration numscript cursor cycle")
		}
		seen[next] = struct{}{}
		cursor = next
	}
	return nil, v3BudgetExceeded("configuration numscript collection exceeded the page limit")
}

func enforceV3ConfigurationJSONBudget(configuration *ledgerconfig.EditableConfig) error {
	encoded, err := json.Marshal(configuration)
	if err != nil {
		return v3Failure("encode configuration budget: %v", err)
	}
	if uint64(len(encoded)) > sdk.DefaultAllPagesMaxBytes {
		return v3BudgetExceeded("configuration exceeded the byte limit")
	}
	return nil
}

type v3NumscriptJSONBudget struct {
	bytes   uint64
	entries map[string]uint64
}

func newV3NumscriptJSONBudget() *v3NumscriptJSONBudget {
	return &v3NumscriptJSONBudget{bytes: 2, entries: make(map[string]uint64)}
}

func (b *v3NumscriptJSONBudget) add(numscript *commonpb.NumscriptInfo) error {
	name, err := json.Marshal(numscript.GetName())
	if err != nil {
		return v3Failure("encode configuration numscript budget: %v", err)
	}
	value, err := json.Marshal(ledgerconfig.EditableNumscript{Content: numscript.GetContent(), Version: numscript.GetVersion()})
	if err != nil {
		return v3Failure("encode configuration numscript budget: %v", err)
	}
	entryBytes := uint64(len(name) + 1 + len(value))
	candidate := b.bytes
	if previous, exists := b.entries[numscript.GetName()]; exists {
		candidate -= previous
	} else if len(b.entries) != 0 {
		candidate++
	}
	if entryBytes > sdk.DefaultAllPagesMaxBytes || candidate > sdk.DefaultAllPagesMaxBytes-entryBytes {
		return v3BudgetExceeded("configuration numscript collection exceeded the byte limit")
	}
	b.bytes = candidate + entryBytes
	b.entries[numscript.GetName()] = entryBytes
	return nil
}

func parseInitialSchema(values []string) ([]*commonpb.SetMetadataFieldTypeCommand, error) {
	result := make([]*commonpb.SetMetadataFieldTypeCommand, 0, len(values))
	for _, value := range values {
		parts := strings.Split(value, ":")
		if len(parts) != 3 || parts[1] == "" {
			return nil, invalidArgument("metadata-type expects target:key:type")
		}
		target, err := commonpb.ParseTargetType(parts[0])
		if err != nil {
			return nil, invalidArgument("invalid schema target")
		}
		metadataType, err := commonpb.ParseMetadataType(parts[2])
		if err != nil {
			return nil, invalidArgument("invalid schema metadata type")
		}
		result = append(result, &commonpb.SetMetadataFieldTypeCommand{TargetType: target, Key: parts[1], Type: metadataType})
	}
	return result, nil
}
