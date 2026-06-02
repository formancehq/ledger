package cmdutil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/explain"
)

// Change represents a single diff entry between original and modified specs.
type Change struct {
	Path string
	Old  any
	New  any
}

// ClearTermBelow clears from cursor to end of screen.
// Fixes display artifacts in iTerm2 when pterm selectors are shown in a loop.
func ClearTermBelow() {
	fmt.Print("\033[J")
}

// clearScreen clears the visible terminal area and moves the cursor to the top-left.
func clearScreen() {
	fmt.Print("\033[2J\033[H")
}

// SelectPrompt wraps pterm interactive select with auto-sizing and iTerm cleanup.
// Filter is only enabled for long lists (>10 options) to avoid rendering artifacts.
func SelectPrompt(label string, options []string) (string, error) {
	maxVisible := min(len(options), 15)

	selector := pterm.DefaultInteractiveSelect.
		WithOptions(options).
		WithDefaultText(label).
		WithMaxHeight(maxVisible).
		WithFilter(len(options) > 10)

	selected, err := selector.Show()
	ClearTermBelow()

	return selected, err
}

// EditLoop shows an interactive menu for the given schema fields and data map.
// isRoot controls whether the sentinel option is "Done" (root) or "Back" (nested).
func EditLoop(fields []explain.Field, data map[string]any, path string, isRoot bool) error {
	sentinel := "Back"
	if isRoot {
		sentinel = "Done"
	}

	for {
		clearScreen()

		options := make([]string, 0, len(fields)+1)
		editableFields := make([]explain.Field, 0, len(fields))

		for _, f := range fields {
			if isSkippedType(f) {
				continue
			}
			label := formatMenuOption(f, data[f.Name])
			options = append(options, label)
			editableFields = append(editableFields, f)
		}

		options = append(options, pterm.Gray("─── "+sentinel+" ───"))

		selected, err := SelectPrompt(path+" > Select a field", options)
		if err != nil {
			return fmt.Errorf("menu selection failed: %w", err)
		}

		if strings.Contains(selected, sentinel) {
			clearScreen()

			return nil
		}

		idx := -1
		for i, opt := range options {
			if opt == selected {
				idx = i

				break
			}
		}
		if idx < 0 || idx >= len(editableFields) {
			continue
		}

		f := editableFields[idx]
		fieldPath := path + "." + f.Name

		switch {
		case isEditableList(f):
			if err := editList(f, data, fieldPath); err != nil {
				return err
			}
		case len(f.Children) > 0:
			sub, ok := data[f.Name].(map[string]any)
			if !ok {
				sub = make(map[string]any)
				data[f.Name] = sub
			}
			if err := EditLoop(f.Children, sub, fieldPath, false); err != nil {
				return err
			}
			if sub, ok := data[f.Name].(map[string]any); ok && len(sub) == 0 {
				delete(data, f.Name)
			}
		default:
			if err := editField(f, data, fieldPath); err != nil {
				return err
			}
		}
	}
}

// editList manages a []object field: add, edit, or remove items.
func editList(f explain.Field, data map[string]any, path string) error {
	items := getSlice(data, f.Name)

	for {
		clearScreen()

		options := make([]string, 0, len(items)+2)
		for i, item := range items {
			summary := listItemSummary(f.Children, item)
			options = append(options, fmt.Sprintf("[%d]  %s", i, summary))
		}
		options = append(options, pterm.Green("+ Add new item"))
		options = append(options, pterm.Gray("─── Back ───"))

		selected, err := SelectPrompt(path+" > Manage list", options)
		if err != nil {
			return fmt.Errorf("list selection failed: %w", err)
		}

		if strings.Contains(selected, "Back") {
			clearScreen()

			break
		}

		if strings.Contains(selected, "Add new item") {
			newItem := make(map[string]any)
			pterm.Info.Printfln("Adding new item to %s", path)
			if err := EditLoop(f.Children, newItem, fmt.Sprintf("%s[%d]", path, len(items)), false); err != nil {
				return err
			}
			if len(newItem) > 0 {
				items = append(items, newItem)
			}

			continue
		}

		idx, err := parseListIndex(selected)
		if err != nil {
			continue
		}
		if idx < 0 || idx >= len(items) {
			continue
		}

		action, err := SelectPrompt(
			fmt.Sprintf("%s[%d] > Action", path, idx),
			[]string{"Edit", pterm.Red("Remove"), pterm.Gray("─── Back ───")},
		)
		if err != nil {
			return fmt.Errorf("action selection failed: %w", err)
		}

		switch {
		case strings.Contains(action, "Back"):
			// Do nothing, re-show list.
		case strings.Contains(action, "Remove"):
			items = append(items[:idx], items[idx+1:]...)
			pterm.Info.Printfln("Removed item [%d]", idx)
		case action == "Edit":
			itemMap, ok := items[idx].(map[string]any)
			if !ok {
				itemMap = make(map[string]any)
			}
			if err := EditLoop(f.Children, itemMap, fmt.Sprintf("%s[%d]", path, idx), false); err != nil {
				return err
			}
			items[idx] = itemMap
		}
	}

	if len(items) == 0 {
		delete(data, f.Name)
	} else {
		data[f.Name] = items
	}

	return nil
}

// editField prompts the user to edit a single leaf field.
func editField(f explain.Field, data map[string]any, path string) error {
	currentVal := data[f.Name]

	pterm.Println()
	pterm.Printf("  %s  %s\n", pterm.Bold.Sprint(path), pterm.Gray(f.Type))
	if f.Description != "" {
		pterm.Printf("  %s\n", f.Description)
	}
	if f.Default != "" {
		pterm.Printf("  Default: %s\n", pterm.Green(f.Default))
	}
	pterm.Printf("  Current: %s\n", formatCurrentValue(currentVal))
	pterm.Println()

	switch f.Type {
	case "bool":
		return editBool(f, data, currentVal)
	case "int32":
		return editInt(f, data, currentVal, 32, path)
	case "int64":
		return editInt(f, data, currentVal, 64, path)
	default:
		return editString(f, data, currentVal)
	}
}

func editBool(f explain.Field, data map[string]any, currentVal any) error {
	var cur *bool
	if currentVal != nil {
		if b, ok := currentVal.(bool); ok {
			cur = &b
		}
	}

	result, err := PromptBool(f.Name, cur)
	if err != nil {
		return err
	}

	if result == nil {
		delete(data, f.Name)
	} else {
		data[f.Name] = *result
	}

	return nil
}

func editInt(f explain.Field, data map[string]any, currentVal any, bitSize int, path string) error {
	defaultStr := ""
	if currentVal != nil {
		defaultStr = FormatNumericValue(currentVal)
	}

	// Validate replicas specially.
	if path == "spec.replicas" {
		replicas, err := PromptReplicas(ParseCurrentInt32(currentVal, 3))
		if err != nil {
			return err
		}
		data[f.Name] = float64(replicas)

		return nil
	}

	input, err := PromptText(f.Name+" (<unset> to clear)", defaultStr)
	if err != nil {
		return err
	}

	input = strings.TrimSpace(input)
	if input == "" || input == "<unset>" {
		delete(data, f.Name)

		return nil
	}

	n, err := strconv.ParseInt(input, 10, bitSize)
	if err != nil {
		pterm.Warning.Printfln("Invalid integer: %s", input)

		return nil
	}
	data[f.Name] = float64(n) // JSON numbers are float64.

	return nil
}

func editString(f explain.Field, data map[string]any, currentVal any) error {
	defaultStr := ""
	if currentVal != nil {
		defaultStr = fmt.Sprintf("%v", currentVal)
	}

	input, err := PromptText(f.Name+" (<unset> to clear)", defaultStr)
	if err != nil {
		return err
	}

	input = strings.TrimSpace(input)
	if input == "" || input == "<unset>" {
		delete(data, f.Name)

		return nil
	}

	data[f.Name] = input

	return nil
}

// formatMenuOption builds a menu line like "fieldName         currentValueSummary".
func formatMenuOption(f explain.Field, val any) string {
	name := f.Name
	summary := fieldSummary(f, val)

	padding := max(30-len(name), 2)

	return name + strings.Repeat(" ", padding) + summary
}

func fieldSummary(f explain.Field, val any) string {
	if isEditableList(f) {
		items := toSlice(val)
		if len(items) == 0 {
			return pterm.Gray("(empty list)")
		}

		return pterm.Gray(fmt.Sprintf("(%d items)", len(items)))
	}

	if len(f.Children) > 0 {
		sub, ok := val.(map[string]any)
		if !ok || len(sub) == 0 {
			return pterm.Gray("(not configured)")
		}
		var names []string
		for _, child := range f.Children {
			if _, exists := sub[child.Name]; exists {
				names = append(names, child.Name)
			}
			if len(names) >= 3 {
				break
			}
		}
		suffix := ""
		count := countConfiguredChildren(f.Children, sub)
		if count > len(names) {
			suffix = fmt.Sprintf(" +%d more", count-len(names))
		}

		return pterm.Gray("▸ " + strings.Join(names, ", ") + suffix)
	}

	if val == nil {
		return pterm.Gray("<not set>")
	}

	return pterm.Green(FormatLeafValue(val))
}

// isEditableList returns true for []object fields that have children
// and can be edited as a list of items.
func isEditableList(f explain.Field) bool {
	return strings.HasPrefix(f.Type, "[]") && len(f.Children) > 0
}

// isSkippedType returns true for types that cannot be edited at all.
func isSkippedType(f explain.Field) bool {
	if isEditableList(f) {
		return false
	}
	if strings.HasPrefix(f.Type, "[]") || strings.HasPrefix(f.Type, "map[") {
		return true
	}
	switch f.Type {
	case "Affinity", "ResourceRequirements", "Probe",
		"PodSecurityContext", "SecurityContext":
		return true
	}

	return false
}

// ComputeDiff recursively compares original and modified maps.
func ComputeDiff(original, modified map[string]any, prefix string) []Change {
	var changes []Change

	for key, newVal := range modified {
		path := prefix + "." + key
		oldVal := original[key]

		newMap, newIsMap := newVal.(map[string]any)
		oldMap, oldIsMap := oldVal.(map[string]any)

		switch {
		case newIsMap && oldIsMap:
			changes = append(changes, ComputeDiff(oldMap, newMap, path)...)
		case newIsMap && !oldIsMap:
			changes = append(changes, ComputeDiff(map[string]any{}, newMap, path)...)
		case !valuesEqual(oldVal, newVal):
			changes = append(changes, Change{Path: path, Old: oldVal, New: newVal})
		}
	}

	for key, oldVal := range original {
		if _, exists := modified[key]; !exists {
			path := prefix + "." + key
			oldMap, oldIsMap := oldVal.(map[string]any)
			if oldIsMap {
				changes = append(changes, ComputeDiff(oldMap, map[string]any{}, path)...)
			} else {
				changes = append(changes, Change{Path: path, Old: oldVal, New: nil})
			}
		}
	}

	return changes
}

func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// DeepCopyMap creates a deep copy of a map[string]any.
func DeepCopyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			cp[k] = DeepCopyMap(sub)
		} else {
			cp[k] = v
		}
	}

	return cp
}

func formatCurrentValue(val any) string {
	if val == nil {
		return pterm.Gray("<not set>")
	}

	return pterm.Green(FormatLeafValue(val))
}

// FormatLeafValue formats a JSON leaf value for display.
func FormatLeafValue(val any) string {
	switch v := val.(type) {
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}

		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// FormatChangeValue formats a value for display in a diff.
func FormatChangeValue(val any) string {
	if val == nil {
		return "<not set>"
	}

	return FormatLeafValue(val)
}

// FormatNumericValue formats a numeric value for display.
func FormatNumericValue(val any) string {
	if f, ok := val.(float64); ok {
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}

		return strconv.FormatFloat(f, 'f', -1, 64)
	}

	return fmt.Sprintf("%v", val)
}

// ParseCurrentInt32 extracts an int32 from a JSON value, or returns fallback.
func ParseCurrentInt32(val any, fallback int32) int32 {
	if val == nil {
		return fallback
	}
	if f, ok := val.(float64); ok {
		return int32(f)
	}

	return fallback
}

func countConfiguredChildren(fields []explain.Field, data map[string]any) int {
	count := 0
	for _, f := range fields {
		if _, exists := data[f.Name]; exists {
			count++
		}
	}

	return count
}

func getSlice(data map[string]any, key string) []any {
	val, ok := data[key]
	if !ok {
		return nil
	}
	slice, ok := val.([]any)
	if !ok {
		return nil
	}

	return slice
}

func toSlice(val any) []any {
	if val == nil {
		return nil
	}
	slice, ok := val.([]any)
	if !ok {
		return nil
	}

	return slice
}

func listItemSummary(children []explain.Field, item any) string {
	m, ok := item.(map[string]any)
	if !ok {
		return pterm.Gray(fmt.Sprintf("%v", item))
	}

	var parts []string
	for _, child := range children {
		if v, exists := m[child.Name]; exists {
			parts = append(parts, fmt.Sprintf("%s: %s", child.Name, FormatLeafValue(v)))
		}
		if len(parts) >= 3 {
			break
		}
	}
	if len(parts) == 0 {
		return pterm.Gray("(empty)")
	}

	return strings.Join(parts, ", ")
}

func parseListIndex(s string) (int, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") {
		return -1, errors.New("no index prefix")
	}
	end := strings.Index(s, "]")
	if end < 0 {
		return -1, errors.New("no closing bracket")
	}

	return strconv.Atoi(s[1:end])
}
