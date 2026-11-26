package cmdutil

import (
	"fmt"
	"strconv"

	"github.com/pterm/pterm"
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
