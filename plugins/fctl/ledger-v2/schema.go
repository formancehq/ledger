package ledgerv2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

const schemaDialect = "https://json-schema.org/draft/2020-12/schema"

// buildInputSchema derives the closed command-v4 input schema from the command
// grammar itself. Deriving instead of transcribing removes the only way the
// two can disagree, which the host rejects as a descriptor defect.
//
// The encoding is byte-deterministic: property names are sorted, so the same
// grammar always produces the same descriptor bytes.
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
			primitive: argumentSchemaPrimitive(argument.Type),
			repeated:  argument.Repeated || argument.Type == sdk.ArgumentStringArray,
			required:  argument.Required,
		}
	}
	for _, flag := range flags {
		fields[flag.Name] = field{
			primitive:    flagSchemaPrimitive(flag.Type),
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

	var properties strings.Builder
	required := make([]string, 0, len(names))
	for index, name := range names {
		value := fields[name]
		if index > 0 {
			properties.WriteByte(',')
		}
		properties.WriteString(strconv.Quote(name))
		properties.WriteByte(':')
		properties.WriteString(propertySchema(value.primitive, value.repeated, value.required, value.hasDefault, value.defaultValue))
		if value.required {
			required = append(required, strconv.Quote(name))
		}
	}

	schema := `{"$schema":"` + schemaDialect + `","type":"object","properties":{` + properties.String() + `}`
	if len(required) > 0 {
		schema += `,"required":[` + strings.Join(required, ",") + `]`
	}
	return []byte(schema + `,"additionalProperties":false}`)
}

func propertySchema(primitive string, repeated, required, hasDefault bool, defaultValue string) string {
	var builder strings.Builder
	if repeated {
		builder.WriteString(`{"type":"array","items":{"type":"` + primitive + `"}`)
		if required {
			builder.WriteString(`,"minItems":1`)
		}
	} else {
		builder.WriteString(`{"type":"` + primitive + `"`)
	}
	if hasDefault {
		builder.WriteString(`,"default":` + schemaDefaultLiteral(primitive, defaultValue))
	}
	builder.WriteByte('}')
	return builder.String()
}

// schemaDefaultLiteral encodes a descriptor default in the JSON form the host
// compares against the grammar. A malformed default is a programming error in
// this package, not caller input, so it panics rather than emitting a
// descriptor the host would reject at install time.
func schemaDefaultLiteral(primitive, value string) string {
	switch primitive {
	case "boolean":
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			panic(fmt.Sprintf("ledger-v2: invalid boolean flag default %q", value))
		}
		return strconv.FormatBool(parsed)
	case "integer":
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("ledger-v2: invalid int32 flag default %q", value))
		}
		return strconv.FormatInt(parsed, 10)
	default:
		return strconv.Quote(value)
	}
}

// objectOutputSchema describes one JSON resource document.
func objectOutputSchema() []byte {
	return []byte(`{"$schema":"` + schemaDialect + `","type":"object"}`)
}

// collectionOutputSchema describes one JSON collection page or, in all-pages
// mode, the complete bounded collection.
func collectionOutputSchema() []byte {
	return []byte(`{"$schema":"` + schemaDialect + `","type":"array","items":{"type":"object"}}`)
}

// binaryOutputSchema describes an opaque payload in the padded base64 view the
// host uses for non-JSON results.
func binaryOutputSchema(mediaType string) []byte {
	return []byte(`{"$schema":"` + schemaDialect + `","type":"string","contentEncoding":"base64","contentMediaType":"` + mediaType + `"}`)
}

func argumentSchemaPrimitive(value sdk.ArgumentType) string {
	switch value {
	case sdk.ArgumentInt32:
		return "integer"
	case sdk.ArgumentBool:
		return "boolean"
	default:
		return "string"
	}
}

func flagSchemaPrimitive(value sdk.FlagType) string {
	switch value {
	case sdk.FlagInt32:
		return "integer"
	case sdk.FlagBool:
		return "boolean"
	default:
		return "string"
	}
}
