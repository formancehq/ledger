package scenario

import "sort"

// ScenarioFunc is a function that provisions a scenario against a Runner.
type ScenarioFunc func(r *Runner) error

var registry = map[string]ScenarioFunc{}

// Register adds a scenario to the global registry. Called from init() in scenario files.
func Register(name string, fn ScenarioFunc) {
	registry[name] = fn
}

// Get returns a registered scenario by name.
func Get(name string) (ScenarioFunc, bool) {
	fn, ok := registry[name]

	return fn, ok
}

// List returns all registered scenario names in sorted order.
func List() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
