package ledgerv3

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// invalidArgument is the only failure a decoding error may produce. It carries
// no caller value: the message names the field, never its content, so a
// malformed secret cannot travel back through a diagnostic.
func invalidArgument(format string, args ...any) error {
	return sdk.Failure{Code: string(sdk.FailureInvalidArgument), Message: fmt.Sprintf(format, args...)}
}

// input is one decoded invocation: every declared field resolved to its value,
// and nothing else. Reads are by the same constant the descriptor declares.
type input struct {
	command sdk.Command
	scalars map[string]string
	lists   map[string][]string
	present map[string]bool
}

// decode binds an ExecuteRequest to the command's declared grammar and fails
// closed on anything the descriptor does not admit: an unknown flag, a missing
// required field, a surplus positional, a repeated scalar, or a value outside a
// declared completion set. The host parses the command line; this is the
// plugin's own refusal to act on input its descriptor never advertised.
func decode(command sdk.Command, request sdk.ExecuteRequest) (input, error) {
	decoded := input{
		command: command,
		scalars: make(map[string]string),
		lists:   make(map[string][]string),
		present: make(map[string]bool),
	}

	arguments := command.Arguments
	positional := request.Arguments
	for index, argument := range arguments {
		repeated := argument.Repeated || argument.Type == sdk.ArgumentStringArray
		if repeated {
			if index != len(arguments)-1 {
				return input{}, invalidArgument("argument %q is repeated but not last", argument.Name)
			}
			values := positional[min(index, len(positional)):]
			if argument.Required && len(values) == 0 {
				return input{}, invalidArgument("argument %q is required", argument.Name)
			}
			if len(values) != 0 {
				decoded.lists[argument.Name] = append([]string(nil), values...)
				decoded.present[argument.Name] = true
			}
			positional = positional[:min(index, len(positional))]
			break
		}
		if index >= len(positional) {
			if argument.Required {
				return input{}, invalidArgument("argument %q is required", argument.Name)
			}
			continue
		}
		decoded.scalars[argument.Name] = positional[index]
		decoded.present[argument.Name] = true
	}

	if !hasRepeatedArgument(arguments) && len(request.Arguments) > len(arguments) {
		return input{}, invalidArgument("command accepts %d arguments, got %d", len(arguments), len(request.Arguments))
	}

	flags := make(map[string]sdk.Flag, len(command.Flags))
	for _, flag := range command.Flags {
		flags[flag.Name] = flag
		for _, alias := range flag.Aliases {
			flags[alias] = flag
		}
	}

	for _, occurrence := range request.Flags {
		flag, declared := flags[occurrence.Name]
		if !declared {
			return input{}, invalidArgument("flag %q is not declared by this command", occurrence.Name)
		}
		if flag.Type == sdk.FlagStringArray {
			decoded.lists[flag.Name] = append(decoded.lists[flag.Name], occurrence.Value)
			decoded.present[flag.Name] = true
			continue
		}
		if decoded.present[flag.Name] {
			return input{}, invalidArgument("flag %q is declared once but was supplied more than once", flag.Name)
		}
		decoded.scalars[flag.Name] = occurrence.Value
		decoded.present[flag.Name] = true
	}

	for _, flag := range command.Flags {
		if !decoded.present[flag.Name] {
			if flag.Required {
				return input{}, invalidArgument("flag %q is required", flag.Name)
			}
			if flag.HasDefault {
				decoded.scalars[flag.Name] = flag.DefaultValue
			}
			continue
		}
		if flag.Type == sdk.FlagBool {
			if _, err := strconv.ParseBool(decoded.scalars[flag.Name]); err != nil {
				return input{}, invalidArgument("flag %q expects a boolean", flag.Name)
			}
		}
		if flag.Type == sdk.FlagInt32 {
			if _, err := strconv.ParseInt(decoded.scalars[flag.Name], 10, 32); err != nil {
				return input{}, invalidArgument("flag %q expects a 32-bit integer", flag.Name)
			}
		}
	}

	if err := decoded.checkCompletionSets(); err != nil {
		return input{}, err
	}
	return decoded, nil
}

func hasRepeatedArgument(arguments []sdk.Argument) bool {
	for _, argument := range arguments {
		if argument.Repeated || argument.Type == sdk.ArgumentStringArray {
			return true
		}
	}
	return false
}

// checkCompletionSets refuses any value outside a static completion set. Those
// sets are exactly the product's closed enumerations, so the descriptor doubles
// as the admissible-value oracle and a new enum member cannot be accepted until
// the descriptor declares it.
func (i input) checkCompletionSets() error {
	check := func(name string, spec sdk.CompletionSpec) error {
		if spec.Kind != sdk.CompletionStatic || !i.present[name] && i.scalars[name] == "" {
			return nil
		}
		value := i.scalars[name]
		for _, candidate := range spec.Candidates {
			if candidate.Value == value {
				return nil
			}
		}
		return invalidArgument("value for %q is not one this command declares", name)
	}
	for _, argument := range i.command.Arguments {
		if err := check(argument.Name, argument.Completion); err != nil {
			return err
		}
	}
	for _, flag := range i.command.Flags {
		if flag.Type == sdk.FlagStringArray {
			continue
		}
		if err := check(flag.Name, flag.Completion); err != nil {
			return err
		}
	}
	return nil
}

// text returns a declared scalar, or the empty string when it was neither
// supplied nor defaulted.
func (i input) text(name string) string { return i.scalars[name] }

// has reports whether the caller supplied the field.
func (i input) has(name string) bool { return i.present[name] }

// list returns a declared repeated field.
func (i input) list(name string) []string { return i.lists[name] }

// boolean returns a declared boolean. decode has already proved it parses.
func (i input) boolean(name string) bool {
	value, _ := strconv.ParseBool(i.scalars[name])
	return value
}

// int32 returns a declared 32-bit integer. decode has already proved it parses.
func (i input) int32(name string) int32 {
	value, _ := strconv.ParseInt(i.scalars[name], 10, 32)
	return int32(value)
}

// uint64 parses a fixed64 wire field. The descriptor grammar has no 64-bit
// integer type, so these arrive as strings: parsing here is the only place the
// full range is enforced, and an out-of-range value is refused rather than
// silently truncated into a different transaction or log.
func (i input) uint64(name string) (uint64, error) {
	raw := strings.TrimSpace(i.scalars[name])
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, invalidArgument("value for %q is not an unsigned 64-bit decimal", name)
	}
	return value, nil
}

// optionalUint64 is uint64 for a field whose absence is meaningful.
func (i input) optionalUint64(name string) (uint64, bool, error) {
	if !i.has(name) && i.scalars[name] == "" {
		return 0, false, nil
	}
	value, err := i.uint64(name)
	return value, err == nil, err
}
