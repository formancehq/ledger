package config

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/explain"
)

type editFlags struct {
	raw bool
}

func newEditCommand(opts *cmdutil.Options) *cobra.Command {
	flags := &editFlags{}

	cmd := &cobra.Command{
		Use:   "edit [name]",
		Short: "Edit Ledger configuration interactively",
		Long:  "Opens an interactive editor to navigate and modify Ledger CRD fields.\nUse --raw to delegate to kubectl edit for full YAML editing.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdit(cmd, opts, flags, args)
		},
	}

	cmd.Flags().BoolVar(&flags.raw, "raw", false, "Delegate to kubectl edit (raw YAML)")

	return cmd
}

func runEdit(cmd *cobra.Command, opts *cmdutil.Options, flags *editFlags, args []string) error {
	ctx := cmd.Context()

	name, ns, err := cmdutil.ResolveLedgerName(ctx, opts, args)
	if err != nil {
		return err
	}

	if flags.raw {
		return runRawEdit(name, ns)
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	ledger, err := cmdutil.GetLedger(ctx, crdClient, ns, name)
	if err != nil {
		return fmt.Errorf("getting ledger %q: %w", name, err)
	}

	// Marshal spec to unstructured map for editing.
	specJSON, err := json.Marshal(ledger.Spec)
	if err != nil {
		return fmt.Errorf("marshaling spec: %w", err)
	}

	var working map[string]any
	if err := json.Unmarshal(specJSON, &working); err != nil {
		return fmt.Errorf("unmarshaling spec: %w", err)
	}

	original := deepCopyMap(working)

	// Header.
	pterm.Println()
	pterm.Printf("Editing Ledger %s (namespace: %s)\n",
		pterm.Bold.Sprint(pterm.Cyan(name)), pterm.Gray(ns))
	cmdutil.Separator()

	// Interactive edit loop.
	if err := editLoop(explain.SpecFields(), working, "spec", true); err != nil {
		return err
	}

	// Compute diff.
	changes := computeDiff(original, working, "spec")
	if len(changes) == 0 {
		pterm.Info.Println("No changes made.")
		return nil
	}

	// Display pending changes.
	pterm.Println()
	pterm.DefaultSection.Println("Pending changes")
	rows := make([][]string, 0, len(changes))
	for _, c := range changes {
		rows = append(rows, []string{
			pterm.Cyan(c.path),
			pterm.Gray(formatChangeValue(c.old)) + "  " + pterm.Yellow("→") + "  " + pterm.Green(formatChangeValue(c.new)),
		})
	}
	cmdutil.RenderTable([]string{"FIELD", "CHANGE"}, rows)

	// Confirm.
	ok, err := cmdutil.PromptConfirm(fmt.Sprintf("Apply changes to Ledger %s?", name), true)
	if err != nil {
		return err
	}
	if !ok {
		pterm.Info.Println("Aborted.")
		return nil
	}

	// Apply: unmarshal modified map back to typed spec.
	modJSON, err := json.Marshal(working)
	if err != nil {
		return fmt.Errorf("marshaling modified spec: %w", err)
	}

	var newSpec ledgerv1alpha1.LedgerSpec
	if err := json.Unmarshal(modJSON, &newSpec); err != nil {
		return fmt.Errorf("unmarshaling modified spec: %w", err)
	}

	patch := client.MergeFrom(ledger.DeepCopy())
	ledger.Spec = newSpec
	if err := crdClient.Patch(ctx, ledger, patch); err != nil {
		return fmt.Errorf("patching ledger %q: %w", name, err)
	}

	pterm.Success.Printfln("Ledger %s updated", name)
	return nil
}

func runRawEdit(name, ns string) error {
	kubectlArgs := []string{"edit", "ledger.ledger.formance.com/" + name, "-n", ns}

	kubectlCmd := exec.Command("kubectl", kubectlArgs...)
	kubectlCmd.Stdin = os.Stdin
	kubectlCmd.Stdout = os.Stdout
	kubectlCmd.Stderr = os.Stderr

	if err := kubectlCmd.Run(); err != nil {
		return fmt.Errorf("kubectl edit failed: %w", err)
	}
	return nil
}

// clearTermBelow clears from cursor to end of screen.
// Fixes display artifacts in iTerm2 when pterm selectors are shown in a loop.
func clearTermBelow() {
	fmt.Print("\033[J")
}

// selectPrompt wraps pterm interactive select with auto-sizing and iTerm cleanup.
func selectPrompt(label string, options []string) (string, error) {
	maxVisible := len(options)
	if maxVisible > 15 {
		maxVisible = 15
	}

	selected, err := pterm.DefaultInteractiveSelect.
		WithOptions(options).
		WithDefaultText(label).
		WithMaxHeight(maxVisible).
		Show()
	clearTermBelow()
	return selected, err
}

// editLoop shows an interactive menu for the given schema fields and data map.
// isRoot controls whether the sentinel option is "Done" (root) or "Back" (nested).
func editLoop(fields []explain.Field, data map[string]any, path string, isRoot bool) error {
	sentinel := "Back"
	if isRoot {
		sentinel = "Done"
	}

	for {
		// Build menu options.
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

		selected, err := selectPrompt(path+" > Select a field", options)
		if err != nil {
			return fmt.Errorf("menu selection failed: %w", err)
		}

		// Sentinel.
		if strings.Contains(selected, sentinel) {
			return nil
		}

		// Find which field was selected.
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
			// Drill into sub-section.
			sub, ok := data[f.Name].(map[string]any)
			if !ok {
				sub = make(map[string]any)
				data[f.Name] = sub
			}
			if err := editLoop(f.Children, sub, fieldPath, false); err != nil {
				return err
			}
			// Clean up empty intermediate maps.
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
		options := make([]string, 0, len(items)+2)
		for i, item := range items {
			summary := listItemSummary(f.Children, item)
			options = append(options, fmt.Sprintf("[%d]  %s", i, summary))
		}
		options = append(options, pterm.Green("+ Add new item"))
		options = append(options, pterm.Gray("─── Back ───"))

		selected, err := selectPrompt(path+" > Manage list", options)
		if err != nil {
			return fmt.Errorf("list selection failed: %w", err)
		}

		if strings.Contains(selected, "Back") {
			break
		}

		if strings.Contains(selected, "Add new item") {
			newItem := make(map[string]any)
			pterm.Info.Printfln("Adding new item to %s", path)
			if err := editLoop(f.Children, newItem, fmt.Sprintf("%s[%d]", path, len(items)), false); err != nil {
				return err
			}
			if len(newItem) > 0 {
				items = append(items, newItem)
			}
			continue
		}

		// Parse item index from "[N]  ...".
		idx, err := parseListIndex(selected)
		if err != nil {
			continue
		}
		if idx < 0 || idx >= len(items) {
			continue
		}

		// Sub-menu: Edit or Remove.
		action, err := selectPrompt(
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
			if err := editLoop(f.Children, itemMap, fmt.Sprintf("%s[%d]", path, idx), false); err != nil {
				return err
			}
			items[idx] = itemMap
		}
	}

	// Write back.
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

	// Show field info.
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
		// string, duration, Quantity — all edited as text.
		return editString(f, data, currentVal)
	}
}

func editBool(f explain.Field, data map[string]any, currentVal any) error {
	var cur *bool
	if currentVal != nil {
		b, ok := currentVal.(bool)
		if ok {
			cur = &b
		}
	}

	result, err := cmdutil.PromptBool(f.Name, cur)
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
		defaultStr = formatNumericValue(currentVal)
	}

	// Validate replicas specially.
	if path == "spec.replicas" {
		replicas, err := cmdutil.PromptReplicas(parseCurrentInt32(currentVal, 3))
		if err != nil {
			return err
		}
		data[f.Name] = float64(replicas)
		return nil
	}

	input, err := cmdutil.PromptText(f.Name+" (<unset> to clear)", defaultStr)
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

	input, err := cmdutil.PromptText(f.Name+" (<unset> to clear)", defaultStr)
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

	// Pad name to 30 chars for alignment.
	padding := 30 - len(name)
	if padding < 2 {
		padding = 2
	}

	return name + strings.Repeat(" ", padding) + summary
}

// fieldSummary returns a short summary of the field's current value.
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
		// Show first few configured child names.
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

	return pterm.Green(formatLeafValue(val))
}

// isEditableList returns true for []object fields that have children
// and can be edited as a list of items.
func isEditableList(f explain.Field) bool {
	return strings.HasPrefix(f.Type, "[]") && len(f.Children) > 0
}

// isSkippedType returns true for types that cannot be edited at all.
func isSkippedType(f explain.Field) bool {
	// Editable lists are handled by editList.
	if isEditableList(f) {
		return false
	}
	// All other array/map types are too complex for field-by-field editing.
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

// change represents a single diff entry.
type change struct {
	path string
	old  any
	new  any
}

// computeDiff recursively compares original and modified maps.
func computeDiff(original, modified map[string]any, prefix string) []change {
	var changes []change

	// Keys in modified.
	for key, newVal := range modified {
		path := prefix + "." + key
		oldVal := original[key]

		newMap, newIsMap := newVal.(map[string]any)
		oldMap, oldIsMap := oldVal.(map[string]any)

		switch {
		case newIsMap && oldIsMap:
			changes = append(changes, computeDiff(oldMap, newMap, path)...)
		case newIsMap && !oldIsMap:
			changes = append(changes, computeDiff(map[string]any{}, newMap, path)...)
		case !valuesEqual(oldVal, newVal):
			changes = append(changes, change{path: path, old: oldVal, new: newVal})
		}
	}

	// Keys removed from original.
	for key, oldVal := range original {
		if _, exists := modified[key]; !exists {
			path := prefix + "." + key
			oldMap, oldIsMap := oldVal.(map[string]any)
			if oldIsMap {
				changes = append(changes, computeDiff(oldMap, map[string]any{}, path)...)
			} else {
				changes = append(changes, change{path: path, old: oldVal, new: nil})
			}
		}
	}

	return changes
}

// valuesEqual compares two values, handling JSON float64 vs int comparisons.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// deepCopyMap creates a deep copy of a map[string]any.
func deepCopyMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			cp[k] = deepCopyMap(sub)
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
	return pterm.Green(formatLeafValue(val))
}

func formatLeafValue(val any) string {
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

func formatChangeValue(val any) string {
	if val == nil {
		return "<not set>"
	}
	return formatLeafValue(val)
}

func formatNumericValue(val any) string {
	if f, ok := val.(float64); ok {
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", val)
}

func parseCurrentInt32(val any, fallback int32) int32 {
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

// getSlice extracts a []any from the data map, or returns nil.
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

// toSlice converts any value to []any if possible.
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

// listItemSummary builds a short description of a list item from its children.
func listItemSummary(children []explain.Field, item any) string {
	m, ok := item.(map[string]any)
	if !ok {
		return pterm.Gray(fmt.Sprintf("%v", item))
	}

	var parts []string
	for _, child := range children {
		if v, exists := m[child.Name]; exists {
			parts = append(parts, fmt.Sprintf("%s: %s", child.Name, formatLeafValue(v)))
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

// parseListIndex extracts the index N from a string starting with "[N]".
func parseListIndex(s string) (int, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") {
		return -1, fmt.Errorf("no index prefix")
	}
	end := strings.Index(s, "]")
	if end < 0 {
		return -1, fmt.Errorf("no closing bracket")
	}
	return strconv.Atoi(s[1:end])
}
