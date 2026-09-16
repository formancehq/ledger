package ledgerv2

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func normalizedLogID(raw json.RawMessage) (string, error) {
	value := strings.TrimSpace(string(raw))
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		var decoded string
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return "", err
		}
		value = decoded
	}
	integer, ok := new(big.Int).SetString(value, 10)
	if !ok || integer.Sign() < 0 {
		return "", fmt.Errorf("invalid log id")
	}
	return integer.String(), nil
}

func commandByID(id string) (sdk.Command, bool) {
	for _, command := range Commands() {
		if command.ID == id {
			return command, true
		}
	}
	return sdk.Command{}, false
}

func collectFlags(command sdk.Command, occurrences []sdk.FlagOccurrence) (map[string][]string, error) {
	declared := make(map[string]sdk.Flag, len(command.Flags))
	for _, flag := range command.Flags {
		declared[flag.Name] = flag
	}
	values := make(map[string][]string, len(occurrences))
	for _, occurrence := range occurrences {
		flag, ok := declared[occurrence.Name]
		if !ok {
			return nil, invalidArgument("unknown flag %q", occurrence.Name)
		}
		if flag.Type != sdk.FlagStringArray && len(values[occurrence.Name]) != 0 {
			return nil, invalidArgument("flag %q is repeated", occurrence.Name)
		}
		switch flag.Type {
		case sdk.FlagInt32:
			parsed, err := strconv.ParseInt(occurrence.Value, 10, 32)
			if err != nil {
				return nil, invalidArgument("flag %q is not an int32", occurrence.Name)
			}
			switch occurrence.Name {
			case "page-size":
				if parsed < 1 || parsed > 1000 {
					return nil, invalidArgument("flag %q must be between 1 and 1000", occurrence.Name)
				}
			case "group-by":
				if parsed < 0 || parsed > 1000 {
					return nil, invalidArgument("flag %q must be between 0 and 1000", occurrence.Name)
				}
			}
		case sdk.FlagBool:
			if _, err := strconv.ParseBool(occurrence.Value); err != nil {
				return nil, invalidArgument("flag %q is not a boolean", occurrence.Name)
			}
		}
		values[occurrence.Name] = append(values[occurrence.Name], occurrence.Value)
	}
	for _, flag := range command.Flags {
		if flag.Required && len(values[flag.Name]) == 0 {
			return nil, invalidArgument("missing required flag %q", flag.Name)
		}
		if len(values[flag.Name]) == 0 && flag.HasDefault {
			values[flag.Name] = []string{flag.DefaultValue}
		}
	}
	return values, nil
}

func parseMetadata(values []string) (map[string]string, error) {
	metadata := make(map[string]string, len(values))
	for _, value := range values {
		key, item, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, invalidArgument("value %q must use key=value", value)
		}
		if _, exists := metadata[key]; exists {
			return nil, invalidArgument("key %q is repeated", key)
		}
		metadata[key] = item
	}
	return metadata, nil
}

func readInputArtifact(ctx context.Context, host sdk.Host, handle string, maxBytes int64) ([]byte, error) {
	var out []byte
	for {
		chunk, err := sdk.ReadInput(ctx, host, handle)
		if err != nil {
			return nil, fmt.Errorf("ledger-v2: read input artifact: %w", err)
		}
		if int64(len(out)+len(chunk.Bytes)) > maxBytes {
			return nil, sdk.Failure{Code: string(sdk.FailureInputTooLarge), Message: "ledger-v2: input artifact is too large"}
		}
		out = append(out, chunk.Bytes...)
		if chunk.Final {
			return out, nil
		}
		if len(chunk.Bytes) == 0 {
			return nil, invalidArgument("input artifact returned an empty non-final chunk")
		}
	}
}

func emitBytes(host sdk.Host, operation string, shape sdk.ResultShape, mediaType string, data []byte, page *sdk.PageInfo) error {
	return host.Emit(sdk.Event{Kind: sdk.EventResult, Result: &sdk.ResultEnvelope{OperationID: operation, Shape: shape, MediaType: mediaType, Data: append([]byte(nil), data...), Page: page}})
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func invalidArgument(format string, arguments ...any) error {
	return sdk.Failure{Code: string(sdk.FailureInvalidArgument), Message: fmt.Sprintf("ledger-v2: "+format, arguments...)}
}

func descriptorInvalid(format string, arguments ...any) error {
	return sdk.Failure{Code: string(sdk.FailureDescriptorInvalid), Message: fmt.Sprintf("ledger-v2: "+format, arguments...)}
}

func budgetExhausted(message string) error {
	return sdk.Failure{Code: string(sdk.FailureBudgetExhausted), Message: "ledger-v2: " + message}
}
