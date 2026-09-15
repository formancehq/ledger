package ledgerv3

import (
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

// schemaDialect is the only JSON Schema dialect the fctl catalogue validator
// accepts.
const schemaDialect = "https://json-schema.org/draft/2020-12/schema"

// buildInputSchema derives the closed input schema from the command grammar so
// the two can never drift. sdk.ValidateCatalogue compares them field by field:
// deriving is the only way to keep all descriptors exactly consistent.
func buildInputSchema(arguments []sdk.Argument, flags []sdk.Flag) []byte {
	type field struct {
		primitive    string
		repeated     bool
		required     bool
		hasDefault   bool
		defaultValue string
	}

	fields := make(map[string]field, len(arguments)+len(flags))
	for _, argument := range arguments {
		fields[argument.Name] = field{
			primitive: argumentPrimitive(argument.Type),
			repeated:  argument.Repeated || argument.Type == sdk.ArgumentStringArray,
			required:  argument.Required,
		}
	}
	for _, flag := range flags {
		fields[flag.Name] = field{
			primitive:    flagPrimitive(flag.Type),
			repeated:     flag.Type == sdk.FlagStringArray,
			required:     flag.Required,
			hasDefault:   flag.HasDefault,
			defaultValue: flag.DefaultValue,
		}
	}

	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	var properties, required []string
	for _, name := range names {
		value := fields[name]
		var property strings.Builder
		if value.repeated {
			property.WriteString(`{"type":"array","items":{"type":` + quote(value.primitive) + `}`)
			if value.required {
				property.WriteString(`,"minItems":1`)
			}
		} else {
			property.WriteString(`{"type":` + quote(value.primitive))
		}
		if value.hasDefault {
			property.WriteString(`,"default":` + schemaDefaultLiteral(value.primitive, value.defaultValue))
		}
		property.WriteString(`}`)
		properties = append(properties, quote(name)+":"+property.String())
		if value.required {
			required = append(required, quote(name))
		}
	}

	var out strings.Builder
	out.WriteString(`{"$schema":` + quote(schemaDialect) + `,"type":"object","properties":{`)
	out.WriteString(strings.Join(properties, ","))
	out.WriteString(`}`)
	if len(required) != 0 {
		out.WriteString(`,"required":[` + strings.Join(required, ",") + `]`)
	}
	out.WriteString(`,"additionalProperties":false}`)
	return []byte(out.String())
}

func argumentPrimitive(value sdk.ArgumentType) string {
	switch value {
	case sdk.ArgumentInt32:
		return "integer"
	case sdk.ArgumentBool:
		return "boolean"
	default:
		return "string"
	}
}

func flagPrimitive(value sdk.FlagType) string {
	switch value {
	case sdk.FlagInt32:
		return "integer"
	case sdk.FlagBool:
		return "boolean"
	default:
		return "string"
	}
}

func schemaDefaultLiteral(primitive, encoded string) string {
	switch primitive {
	case "integer", "boolean":
		return encoded
	default:
		return quote(encoded)
	}
}

// quote emits a JSON string for the catalogue's own identifiers, which the SDK
// already restricts to a safe token alphabet.
func quote(value string) string {
	return strconv.Quote(value)
}
