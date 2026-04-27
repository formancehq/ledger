package accounttype

const maxBindings = 10

type binding struct {
	name  string
	value string
}

// Bindings stores variable captures from MatchAddress.
// It is a value type (no heap allocation) with inline storage for up to 10 variables.
type Bindings struct {
	entries [maxBindings]binding
	len     int
}

func (b *Bindings) set(name, value string) {
	b.entries[b.len] = binding{name: name, value: value}
	b.len++
}

// Get returns the captured value for a variable name.
func (b *Bindings) Get(name string) (string, bool) {
	for i := range b.len {
		if b.entries[i].name == name {
			return b.entries[i].value, true
		}
	}

	return "", false
}
