package numscriptgen

import (
	"fmt"
	"math/big"
)

// accountRenamer maps the generator's pool names (acc0..accN) to the caller's
// addresses. Any other name, world included, is kept.
type accountRenamer map[string]string

func newAccountRenamer(accounts []string) accountRenamer {
	if len(accounts) < maxPoolSize {
		panic(fmt.Sprintf("numscriptgen: need at least %d accounts, got %d", maxPoolSize, len(accounts)))
	}

	r := make(accountRenamer, maxPoolSize)
	for i := range maxPoolSize {
		r[fmt.Sprintf("acc%d", i)] = accounts[i]
	}

	return r
}

func (r accountRenamer) name(account string) string {
	if renamed, ok := r[account]; ok {
		return renamed
	}

	return account
}

// script renames every account occurrence of s in place. An account-typed
// metadata value is the only metadata value spelled like a pool name, so
// values are renamed by the same lookup.
func (r accountRenamer) script(s *Script) {
	for i := range s.Vars {
		s.Vars[i].Account = r.name(s.Vars[i].Account)
	}

	for i := range s.AccountVars {
		s.AccountVars[i].Value = r.name(s.AccountVars[i].Value)
	}

	r.program(s.Seeds)
	r.program(s.Program)

	for i := range s.Extra {
		r.extra(&s.Extra[i])
	}

	if s.Focus != nil {
		s.Focus.Account = r.name(s.Focus.Account)
	}

	for _, st := range s.Scenario {
		if st.Send != nil {
			r.statement(st.Send)
		}

		if st.Extra != nil {
			r.extra(st.Extra)
		}
	}

	balances := make(map[BalanceKey]*big.Int, len(s.Balances))
	for k, v := range s.Balances {
		balances[BalanceKey{Account: r.name(k.Account), Asset: k.Asset}] = v
	}

	s.Balances = balances

	metadata := make(map[MetaKey]string, len(s.Metadata))
	for k, v := range s.Metadata {
		metadata[MetaKey{Account: r.name(k.Account), Key: k.Key}] = r.name(v)
	}

	s.Metadata = metadata
}

func (r accountRenamer) program(p Program) {
	for i := range p {
		r.statement(&p[i])
	}
}

func (r accountRenamer) statement(st *Statement) {
	r.source(&st.Source)
	r.destination(&st.Destination)
}

func (r accountRenamer) extra(e *ExtraStatement) {
	e.Account = r.name(e.Account)
	e.Destination = r.name(e.Destination)

	if e.Literal != nil {
		e.Literal.Account = r.name(e.Literal.Account)
	}
}

func (r accountRenamer) source(s *Source) {
	s.Account = r.name(s.Account)

	if s.Inner != nil {
		r.source(s.Inner)
	}

	for i := range s.Sources {
		r.source(&s.Sources[i])
	}

	for i := range s.Clauses {
		r.source(&s.Clauses[i].Source)
	}

	if s.AllotmentRemaining != nil {
		r.source(s.AllotmentRemaining)
	}
}

func (r accountRenamer) destination(d *Destination) {
	d.Account = r.name(d.Account)

	for i := range d.InorderClauses {
		r.keptOrDest(&d.InorderClauses[i].KeptOrDest)
	}

	if d.Remaining != nil {
		r.keptOrDest(d.Remaining)
	}

	for i := range d.AllotClauses {
		r.keptOrDest(&d.AllotClauses[i].KeptOrDest)
	}

	if d.AllotRemaining != nil {
		r.keptOrDest(d.AllotRemaining)
	}
}

func (r accountRenamer) keptOrDest(k *KeptOrDest) {
	if k.Dest != nil {
		r.destination(k.Dest)
	}
}
