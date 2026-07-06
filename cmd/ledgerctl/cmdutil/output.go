package cmdutil

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var (
	protoMessageType  = reflect.TypeFor[proto.Message]()
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	protoJSONOpts     = protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}
)

// AddOutputFlags registers --json and --yaml flags on the command.
// The two flags are mutually exclusive.
func AddOutputFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Bool("yaml", false, "Output as YAML")
	cmd.MarkFlagsMutuallyExclusive("json", "yaml")
}

// EncodeStructured checks whether --json or --yaml is set. When one is active
// it encodes data to os.Stdout and returns (true, nil) on success or
// (true, err) on failure. When neither flag is set it returns (false, nil) so
// the caller can fall through to its pterm rendering.
//
// When --result-file is also set the JSON payload is additionally written to
// that path. The flag is a generic out-of-band sink — any wrapper (a CI
// script, the ledger-operator pointing at /dev/termination-log, ...) can
// opt in without parsing stdout.
func EncodeStructured(cmd *cobra.Command, data any) (bool, error) {
	if jsonOutput, _ := cmd.Flags().GetBool("json"); jsonOutput {
		b, err := marshalJSON(data)
		if err != nil {
			return true, err
		}

		if _, err := os.Stdout.Write(append(b, '\n')); err != nil {
			return true, err
		}

		if path, _ := cmd.Flags().GetString("result-file"); path != "" {
			if err := writeResultFile(path, b); err != nil {
				return true, err
			}
		}

		return true, nil
	}

	if yamlOutput, _ := cmd.Flags().GetBool("yaml"); yamlOutput {
		return true, encodeYAMLViaJSON(data)
	}

	return false, nil
}

// writeResultFile writes the JSON payload to path. The file is truncated and
// written in one shot to keep the result self-contained (kubelet reads
// /dev/termination-log in a single pass at container exit, so partial
// writes would be surfaced as truncated JSON on pod.status.).
func writeResultFile(path string, payload []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return fmt.Errorf("opening result file %q: %w", path, err)
	}

	if _, writeErr := f.Write(payload); writeErr != nil {
		_ = f.Close()

		return fmt.Errorf("writing result file %q: %w", path, writeErr)
	}

	if closeErr := f.Close(); closeErr != nil {
		return fmt.Errorf("closing result file %q: %w", path, closeErr)
	}

	return nil
}

// encodeYAMLViaJSON marshals data to JSON first (using protojson for proto
// types), then converts to YAML. This ensures YAML keys use camelCase from
// protobuf canonical JSON rather than lowercased Go field names.
func encodeYAMLViaJSON(data any) error {
	jsonBytes, err := marshalJSON(data)
	if err != nil {
		return err
	}

	var intermediate any
	if err := json.Unmarshal(jsonBytes, &intermediate); err != nil {
		return err
	}

	encoder := yaml.NewEncoder(os.Stdout)
	encoder.SetIndent(2)

	err = encoder.Encode(intermediate)
	if closeErr := encoder.Close(); err == nil {
		err = closeErr
	}

	return err
}

// marshalJSON dispatches to the appropriate JSON encoder:
//   - proto.Message with custom MarshalJSON → encoding/json (respects custom marshalers for Uint256, Timestamp, etc.)
//   - proto.Message without custom MarshalJSON → protojson (preserves camelCase field names)
//   - slices/maps of proto types → same logic per element
//   - everything else → encoding/json
func marshalJSON(data any) ([]byte, error) {
	// Direct proto.Message
	if msg, ok := data.(proto.Message); ok {
		if isNilProto(msg) {
			return []byte("null"), nil
		}

		// Prefer custom MarshalJSON when available (handles Uint256, Timestamp, etc.)
		if _, ok := data.(json.Marshaler); ok {
			return json.MarshalIndent(data, "", "  ")
		}

		return protoJSONOpts.Marshal(msg)
	}

	rv := reflect.ValueOf(data)
	if !rv.IsValid() {
		return json.MarshalIndent(data, "", "  ")
	}

	switch rv.Kind() {
	case reflect.Slice:
		elemType := rv.Type().Elem()
		// If elements have custom MarshalJSON, encoding/json will call it for each element.
		if elemType.Implements(jsonMarshalerType) {
			return json.MarshalIndent(data, "", "  ")
		}

		if elemType.Implements(protoMessageType) {
			return marshalProtoSlice(rv)
		}
	case reflect.Map:
		// map[string]any — inspect values individually for proto types
		if rv.Type().Key().Kind() == reflect.String && rv.Type().Elem().Kind() == reflect.Interface {
			return marshalMapStringAny(rv)
		}
		if rv.Type().Elem().Implements(protoMessageType) {
			return marshalProtoMap(rv)
		}
	}

	return json.MarshalIndent(data, "", "  ")
}

// marshalProtoSlice marshals a slice of proto.Message values.
func marshalProtoSlice(rv reflect.Value) ([]byte, error) {
	items := make([]any, rv.Len())
	for i := range rv.Len() {
		elem := rv.Index(i)
		msg, ok := elem.Interface().(proto.Message)
		if !ok || isNilProto(msg) {
			items[i] = nil

			continue
		}
		v, err := protoToAny(msg)
		if err != nil {
			return nil, err
		}
		items[i] = v
	}

	return json.MarshalIndent(items, "", "  ")
}

// marshalProtoMap marshals a map with proto.Message values.
func marshalProtoMap(rv reflect.Value) ([]byte, error) {
	result := make(map[string]any, rv.Len())
	for _, key := range rv.MapKeys() {
		msg, ok := rv.MapIndex(key).Interface().(proto.Message)
		if !ok || isNilProto(msg) {
			result[key.String()] = nil

			continue
		}
		v, err := protoToAny(msg)
		if err != nil {
			return nil, err
		}
		result[key.String()] = v
	}

	return json.MarshalIndent(result, "", "  ")
}

// marshalMapStringAny handles map[string]any by inspecting each value and
// converting proto.Message or proto slices via protojson.
func marshalMapStringAny(rv reflect.Value) ([]byte, error) {
	result := make(map[string]any, rv.Len())
	for _, key := range rv.MapKeys() {
		val := rv.MapIndex(key).Elem()
		converted, err := convertAnyValue(val)
		if err != nil {
			return nil, err
		}
		result[key.String()] = converted
	}

	return json.MarshalIndent(result, "", "  ")
}

// convertAnyValue converts a reflected value, handling proto.Message, slices
// of proto.Message, and passing through everything else.
func convertAnyValue(rv reflect.Value) (any, error) {
	if !rv.IsValid() {
		return nil, nil
	}

	iface := rv.Interface()

	// Direct proto.Message
	if msg, ok := iface.(proto.Message); ok {
		if isNilProto(msg) {
			return nil, nil
		}

		return protoToAny(msg)
	}

	// Slice with proto element type
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Implements(protoMessageType) {
		items := make([]any, rv.Len())
		for i := range rv.Len() {
			msg, ok := rv.Index(i).Interface().(proto.Message)
			if !ok || isNilProto(msg) {
				items[i] = nil

				continue
			}
			v, err := protoToAny(msg)
			if err != nil {
				return nil, err
			}
			items[i] = v
		}

		return items, nil
	}

	return iface, nil
}

// protoToAny marshals a proto.Message to JSON, then unmarshals to any so it
// can be combined with other values in a json.MarshalIndent call.
// Prefers custom MarshalJSON when available (handles Uint256, Timestamp, etc.).
func protoToAny(msg proto.Message) (any, error) {
	var b []byte
	var err error

	if jm, ok := msg.(json.Marshaler); ok {
		b, err = jm.MarshalJSON()
	} else {
		b, err = protoJSONOpts.Marshal(msg)
	}

	if err != nil {
		return nil, err
	}

	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}

	return v, nil
}

// isNilProto returns true if the proto.Message is a nil pointer.
func isNilProto(msg proto.Message) bool {
	rv := reflect.ValueOf(msg)

	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

// IsStructuredOutput returns true when --json or --yaml is active.
// Use this for paginated commands that need to skip interactive prompts.
func IsStructuredOutput(cmd *cobra.Command) bool {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	yamlOutput, _ := cmd.Flags().GetBool("yaml")

	return jsonOutput || yamlOutput
}

// EmitNextCursorHint surfaces the resume cursor to the user without
// contaminating the structured payload on stdout.
//
//   - human mode (no --json/--yaml): prints a pterm.Info line to stdout.
//   - structured mode (--json/--yaml): prints a single `next_cursor=<token>`
//     line to stderr so scripts that pipe stdout into `jq` / `yq` still get a
//     parseable payload while keeping the resume hint reachable.
//
// Pass an empty cursor to skip emission entirely.
func EmitNextCursorHint(cmd *cobra.Command, nextCursor string) {
	if nextCursor == "" {
		return
	}

	if IsStructuredOutput(cmd) {
		// Stderr keeps stdout JSON/YAML lossless for downstream parsers.
		// The hint is best-effort — a closed stderr (rare) is not fatal.
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "next_cursor=%s\n", nextCursor)

		return
	}

	pterm.Info.Printfln("More results available — resume with --cursor %s", pterm.Cyan(nextCursor))
}
