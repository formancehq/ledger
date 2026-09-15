package ledgerv3

import (
	"context"
	"sort"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/protobuf/proto"
)

func executeV3AccountTypes(ctx context.Context, _ sdk.ExecuteRequest, decoded input, command sdk.Command, host sdk.Host) (bool, error) {
	switch command.ID {
	case "ledger.v3.account-types.add":
		persistence, err := commonpb.ParsePersistence(decoded.text(flagPersistence))
		if err != nil {
			return true, invalidArgument("invalid persistence mode")
		}
		segments, err := parseSegmentTypes(decoded.list(flagSegmentType))
		if err != nil {
			return true, err
		}
		_, err = applyV3(ctx, host, command, opApplyAddAccountType.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
			Ledger: decoded.text(argLedger), AccountType: &commonpb.AccountType{Name: decoded.text(argName), Pattern: decoded.text(flagPattern), Persistence: persistence, SegmentTypes: segments},
		}}})
		if err != nil {
			return true, err
		}
		return true, emitEmpty(host, opApplyAddAccountType.id)
	case "ledger.v3.account-types.remove":
		_, err := applyV3(ctx, host, command, opApplyRemoveAccountType.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: decoded.text(argLedger), Name: decoded.text(argName)}}})
		if err != nil {
			return true, err
		}
		return true, emitEmpty(host, opApplyRemoveAccountType.id)
	case "ledger.v3.account-types.set-default-enforcement":
		mode := commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
		if decoded.text(flagEnforcementMode) == "strict" {
			mode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		}
		_, err := applyV3(ctx, host, command, opApplyDefaultEnforcement.id, decoded.text(flagIdempotencyKey), &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: decoded.text(argLedger), EnforcementMode: mode}}})
		if err != nil {
			return true, err
		}
		return true, emitEmpty(host, opApplyDefaultEnforcement.id)
	case "ledger.v3.account-types.get", "ledger.v3.account-types.list":
		checkpoint, _, err := decoded.optionalUint64(flagCheckpointID)
		if err != nil {
			return true, err
		}
		ledger, err := unaryV3(ctx, host, command, opGetLedger.id, &servicepb.GetLedgerRequest{Ledger: decoded.text(argLedger), Read: &commonpb.ReadOptions{CheckpointId: checkpoint}}, &commonpb.LedgerInfo{})
		if err != nil {
			return true, v3Failure("get ledger account types: %v", err)
		}
		if command.ID == "ledger.v3.account-types.get" {
			accountType := ledger.GetAccountTypes()[decoded.text(argName)]
			if accountType == nil {
				return true, v3Failure("account type not found")
			}
			return true, emitProto(host, opGetLedger.id, accountType)
		}
		names := make([]string, 0, len(ledger.GetAccountTypes()))
		for name := range ledger.GetAccountTypes() {
			names = append(names, name)
		}
		sort.Strings(names)
		items := make([]proto.Message, 0, len(names))
		for _, name := range names {
			items = append(items, ledger.GetAccountTypes()[name])
		}
		return true, emitProtoList(host, opGetLedger.id, items, nil)
	default:
		return false, nil
	}
}

func parseSegmentTypes(values []string) (map[string]*commonpb.SegmentType, error) {
	result := make(map[string]*commonpb.SegmentType, len(values))
	for _, value := range values {
		name, constraint, ok := strings.Cut(value, "=")
		if !ok || name == "" || constraint == "" {
			return nil, invalidArgument("segment-type expects variable=constraint")
		}
		if _, exists := result[name]; exists {
			return nil, invalidArgument("segment type %q is repeated", name)
		}
		typeName, regex := constraint, ""
		if strings.HasPrefix(constraint, "regex:") {
			typeName, regex = commonpb.SegmentTypeRegex, strings.TrimPrefix(constraint, "regex:")
		}
		segment, err := commonpb.SegmentTypeFromJSON(&commonpb.SegmentTypeJSON{Type: typeName, Regex: regex})
		if err != nil {
			return nil, invalidArgument("invalid segment type for %q", name)
		}
		result[name] = segment
	}
	return result, nil
}
