package internal

import "maps"

// Details is a map of key-value pairs used for Antithesis assertion context.
type Details map[string]any

// With returns a new Details with additional entries merged in.
func (d Details) With(extra Details) Details {
	out := make(Details)
	maps.Copy(out, d)
	maps.Copy(out, extra)
	return out
}
