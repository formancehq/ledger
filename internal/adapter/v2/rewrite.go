package v2

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// AddressRewriter applies a mirror's configured address rewrite rules to every
// account address while v2 logs are translated into v3 orders. It exists so
// segments that v2 only carried as lock-avoidance shards (e.g. ":worker:001")
// can be dropped or renamed as history is replayed into a v3 mirror ledger.
//
// Rewriting is a pure translation-time projection: the source v2 ledger is
// untouched, and the rewritten addresses are baked into the Raft order proposed
// by the (single) leader, so followers apply identical addresses — no
// cross-node determinism concern. Because coverage/preload and the FSM read the
// already-translated payload, rewriting here needs no changes anywhere else.
type AddressRewriter struct {
	rules []compiledRule
}

type compiledRule struct {
	re          *regexp.Regexp
	replacement string
}

// NewAddressRewriter compiles the given rules. It returns a nil rewriter when
// there are no rules; a nil *AddressRewriter is a valid pass-through (Rewrite is
// nil-safe).
func NewAddressRewriter(rules []*commonpb.AddressRewriteRule) (*AddressRewriter, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	compiled := make([]compiledRule, 0, len(rules))
	for _, rule := range rules {
		// An empty pattern matches at every boundary and would rewrite every
		// address; admission rejects it, but guard here too since this is the
		// single point every rule is compiled.
		if rule.GetPattern() == "" {
			return nil, errors.New("address rewrite pattern must not be empty")
		}

		re, err := regexp.Compile(rule.GetPattern())
		if err != nil {
			return nil, fmt.Errorf("compiling address rewrite pattern %q: %w", rule.GetPattern(), err)
		}

		compiled = append(compiled, compiledRule{re: re, replacement: rule.GetReplacement()})
	}

	return &AddressRewriter{rules: compiled}, nil
}

// Rewrite applies every rule, in order, to the address. A nil receiver (no
// rules) returns the address unchanged. When a rule actually changes the
// address, the result is validated as a ledger account address so an invalid
// rewrite fails the batch loudly rather than corrupting the mirrored ledger.
func (r *AddressRewriter) Rewrite(address string) (string, error) {
	if r == nil {
		return address, nil
	}

	rewritten := address
	for _, rule := range r.rules {
		rewritten = rule.re.ReplaceAllString(rewritten, rule.replacement)
	}

	if rewritten == address {
		// Unchanged: the address came from the source ledger and is already valid.
		return address, nil
	}

	if err := invariants.ValidateLedgerAccountAddress(rewritten); err != nil {
		return "", fmt.Errorf("address rewrite of %q produced invalid address %q: %w", address, rewritten, err)
	}

	return rewritten, nil
}
