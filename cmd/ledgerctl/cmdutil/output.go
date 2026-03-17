package cmdutil

import (
	"encoding/json"
	"os"
	"reflect"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var (
	protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()
	protoJSONOpts    = protojson.MarshalOptions{
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
func EncodeStructured(cmd *cobra.Command, data any) (bool, error) {
	if jsonOutput, _ := cmd.Flags().GetBool("json"); jsonOutput {
		b, err := marshalJSON(data)
		if err != nil {
			return true, err
		}
		_, err = os.Stdout.Write(append(b, '\n'))
		return true, err
	}

	if yamlOutput, _ := cmd.Flags().GetBool("yaml"); yamlOutput {
		return true, encodeYAMLViaJSON(data)
	}

	return false, nil
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

// marshalJSON dispatches to protojson for proto.Message types, handles slices
// and maps containing proto types, and falls back to encoding/json otherwise.
func marshalJSON(data any) ([]byte, error) {
	// Direct proto.Message
	if msg, ok := data.(proto.Message); ok {
		if isNilProto(msg) {
			return []byte("null"), nil
		}
		return protoJSONOpts.Marshal(msg)
	}

	rv := reflect.ValueOf(data)
	if !rv.IsValid() {
		return json.MarshalIndent(data, "", "  ")
	}

	switch rv.Kind() {
	case reflect.Slice:
		if rv.Type().Elem().Implements(protoMessageType) {
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

// protoToAny marshals a proto.Message to canonical JSON, then unmarshals to
// any so it can be combined with other values in a json.MarshalIndent call.
func protoToAny(msg proto.Message) (any, error) {
	b, err := protoJSONOpts.Marshal(msg)
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
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// IsStructuredOutput returns true when --json or --yaml is active.
// Use this for paginated commands that need to skip interactive prompts.
func IsStructuredOutput(cmd *cobra.Command) bool {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	yamlOutput, _ := cmd.Flags().GetBool("yaml")

	return jsonOutput || yamlOutput
}
