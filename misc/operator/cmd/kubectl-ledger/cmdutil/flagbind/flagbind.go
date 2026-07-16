package flagbind

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/explain"
)

// SchemaFunc returns the explain fields to use for --set completion.
type SchemaFunc func() []explain.Field

// RegisterSetFlag adds a --set flag (repeatable) to the command with
// path completion from the explain schema.
// An optional SchemaFunc can be passed to override the default (SpecFields).
func RegisterSetFlag(cmd *cobra.Command, values *[]string, schemaFns ...SchemaFunc) {
	cmd.Flags().StringArrayVar(values, "set", nil,
		"Set a field using dotted path (e.g. --set ingress.tls[0].secretName=my-secret)")

	fn := explain.SpecFields
	if len(schemaFns) > 0 && schemaFns[0] != nil {
		fn = schemaFns[0]
	}

	_ = cmd.RegisterFlagCompletionFunc("set", makeSetCompletionFunc(fn))
}

// Collect parses --set values and returns a nested map.
func Collect(setValues []string) (map[string]any, error) {
	if len(setValues) == 0 {
		return make(map[string]any), nil
	}

	return ParseSetValues(setValues)
}

// ApplyToStruct merges overrides into a typed CRD spec struct via JSON
// marshal → deep-merge → unmarshal round-trip.
// String values from --set are coerced to match the target Go field types
// using reflection before marshaling, so "true" becomes bool for bool fields
// but stays string for string fields.
func ApplyToStruct(target any, overrides map[string]any) error {
	if len(overrides) == 0 {
		return nil
	}

	baseJSON, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("marshaling base: %w", err)
	}

	var base map[string]any
	if err := json.Unmarshal(baseJSON, &base); err != nil {
		return fmt.Errorf("unmarshaling base: %w", err)
	}

	DeepMerge(base, overrides)

	// Coerce string values to match target struct field types.
	coerceToSchema(base, reflect.TypeOf(target))

	mergedJSON, err := json.Marshal(base)
	if err != nil {
		return fmt.Errorf("marshaling merged: %w", err)
	}

	return json.Unmarshal(mergedJSON, target)
}

// coerceToSchema walks a map and coerces string values to match the
// corresponding Go struct field types (bool, int32, float64, etc.).
// String fields are left as-is.
func coerceToSchema(m map[string]any, t reflect.Type) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}

	// Build a map of json tag → field type.
	fieldTypes := make(map[string]reflect.StructField)
	for sf := range t.Fields() {
		tag := sf.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		jsonName := strings.Split(tag, ",")[0]
		fieldTypes[jsonName] = sf
	}

	for key, val := range m {
		sf, ok := fieldTypes[key]
		if !ok {
			continue
		}

		ft := sf.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		switch val := val.(type) {
		case string:
			m[key] = coerceString(val, ft)
		case map[string]any:
			if ft.Kind() == reflect.Map && ft.Key().Kind() == reflect.String {
				// Target is map[string]T — coerce values to T.
				coerceMapTo(val, ft.Elem())
			} else {
				coerceToSchema(val, ft)
			}
		case []any:
			coerceSlice(val, ft)
		}
	}
}

// coerceString converts a string value to the appropriate Go JSON type.
func coerceString(s string, ft reflect.Type) any {
	switch ft.Kind() {
	case reflect.Bool:
		if s == "true" {
			return true
		}
		if s == "false" {
			return false
		}
	case reflect.Int32, reflect.Int64, reflect.Int:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return float64(n) // JSON numbers are float64
		}
	case reflect.Float32, reflect.Float64:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	// String fields and unrecognized types: keep as string.
	return s
}

// coerceMapTo coerces values of a map[string]any to match the target value type.
// For map[string]string targets, string values are kept as-is.
func coerceMapTo(m map[string]any, valType reflect.Type) {
	for k, v := range m {
		if s, ok := v.(string); ok {
			m[k] = coerceString(s, valType)
		}
	}
}

// coerceSlice coerces elements of a []any based on the slice element type.
func coerceSlice(arr []any, ft reflect.Type) {
	if ft.Kind() != reflect.Slice {
		return
	}
	elemType := ft.Elem()
	if elemType.Kind() == reflect.Pointer {
		elemType = elemType.Elem()
	}

	for i, item := range arr {
		switch val := item.(type) {
		case string:
			arr[i] = coerceString(val, elemType)
		case map[string]any:
			if elemType.Kind() == reflect.Struct {
				coerceToSchema(val, elemType)
			}
		}
	}
}

// SetNested sets a value in a nested map, creating intermediate maps as needed.
func SetNested(m map[string]any, parts []string, val any) {
	for i, part := range parts {
		if i == len(parts)-1 {
			m[part] = val

			return
		}
		sub, ok := m[part].(map[string]any)
		if !ok {
			sub = make(map[string]any)
			m[part] = sub
		}
		m = sub
	}
}

// DeepMerge recursively merges src into dst. src wins on conflicts.
func DeepMerge(dst, src map[string]any) {
	for key, srcVal := range src {
		dstVal, exists := dst[key]
		if !exists {
			dst[key] = srcVal

			continue
		}

		srcMap, srcIsMap := srcVal.(map[string]any)
		dstMap, dstIsMap := dstVal.(map[string]any)

		if srcIsMap && dstIsMap {
			DeepMerge(dstMap, srcMap)
		} else {
			dst[key] = srcVal
		}
	}
}

// PreviewRows returns [][]string rows for display in a boxed table.
// Optional skipKeys are top-level keys to omit (e.g. fields already displayed explicitly).
func PreviewRows(overrides map[string]any, prefix string, skipKeys ...string) [][]string {
	skip := make(map[string]bool, len(skipKeys))
	for _, k := range skipKeys {
		skip[k] = true
	}

	var rows [][]string
	for key, val := range overrides {
		if prefix == "" && skip[key] {
			continue
		}
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if sub, ok := val.(map[string]any); ok {
			rows = append(rows, PreviewRows(sub, path)...)
		} else {
			rows = append(rows, []string{path, fmt.Sprintf("%v", val)})
		}
	}

	return rows
}

// makeSetCompletionFunc returns a completion function that uses the given schema provider.
func makeSetCompletionFunc(schemaFn SchemaFunc) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return completeSetPaths(schemaFn(), toComplete)
	}
}

// completeSetPaths suggests field paths for --set completion.
func completeSetPaths(schema []explain.Field, toComplete string) ([]string, cobra.ShellCompDirective) {
	// Already typing a value (after =) — no completion.
	if strings.ContainsRune(toComplete, '=') {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	// Navigate to the current position in the schema tree.
	parts := strings.Split(toComplete, ".")
	prefix := ""
	fields := schema

	for i := range len(parts) - 1 {
		segment := parts[i]
		if idx := strings.IndexByte(segment, '['); idx >= 0 {
			segment = segment[:idx]
		}

		found := false
		for _, f := range fields {
			if f.Name == segment {
				fields = f.Children
				found = true

				break
			}
		}
		if !found {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		if prefix != "" {
			prefix += "."
		}
		prefix += parts[i]
	}

	skip := map[string]bool{
		"Affinity": true, "Probe": true, "PodSecurityContext": true,
		"SecurityContext": true, "ResourceRequirements": true,
	}

	var completions []string
	for _, f := range fields {
		if skip[f.Type] {
			continue
		}

		candidate := f.Name
		if prefix != "" {
			candidate = prefix + "." + f.Name
		}
		if !strings.HasPrefix(candidate, toComplete) {
			continue
		}

		if len(f.Children) > 0 {
			completions = append(completions, candidate+".")
		} else {
			completions = append(completions, candidate+"=")
		}
	}

	return completions, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}
