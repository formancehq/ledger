package auth

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadScopeMappingFromFile reads a scope mapping JSON file and returns the parsed ScopeMapping.
func LoadScopeMappingFromFile(path string) (ScopeMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading scope mapping file: %w", err)
	}

	return ParseScopeMappingJSON(data)
}

// ParseScopeMappingJSON parses a JSON-encoded scope mapping.
// Expected format: {"ledger:read": ["ledgers:read", "transactions:read", ...], ...}.
func ParseScopeMappingJSON(data []byte) (ScopeMapping, error) {
	var raw map[string][]string

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("parsing scope mapping JSON: %w", err)
	}

	mapping := make(ScopeMapping, len(raw))
	for virtualScope, granularScopes := range raw {
		scopes := make([]Scope, len(granularScopes))
		for i, s := range granularScopes {
			scope := Scope(s)
			if _, ok := AllGranularScopes[scope]; !ok {
				return nil, fmt.Errorf("unknown granular scope %q in mapping for %q", s, virtualScope)
			}

			scopes[i] = scope
		}

		mapping[virtualScope] = scopes
	}

	return mapping, nil
}
