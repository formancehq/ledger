package numscriptgen

// cleanupProgram is a port of Numscript.Utils.cleanupNumscript: it removes
// constructs from a generated program that the legacy machine would reject
// at compile time. Only each statement's Source is touched (Destination is
// left as-is — that matches the original, which never cleans destinations).
func cleanupProgram(p Program) Program {
	out := make(Program, len(p))
	for i, stmt := range p {
		out[i] = cleanupStatement(stmt)
	}
	return out
}

func cleanupStatement(s Statement) Statement {
	emptiedAccounts := map[string]bool{}
	cleaned := cleanupSrc(s.Source, emptiedAccounts)

	if final := removeEmptyAllotmentsSrc(cleaned); final != nil {
		s.Source = *final
	} else {
		// Port of `fromMaybe "world"`: if the whole source tree collapsed
		// to nothing, fall back to a plain `@world` source.
		s.Source = Source{Kind: SrcAccount, Account: "world"}
	}

	return s
}

// isSrcUnbounded is true only for a literal `world` account or an explicit
// unbounded overdraft. Notably, a
// SrcCapped wrapping an unbounded source is NOT itself unbounded — the cap
// always bounds it.
func isSrcUnbounded(s Source) bool {
	switch s.Kind {
	case SrcAccount:
		return s.Account == "world"
	case SrcAccountOverdraft:
		return s.Overdraft == nil
	default:
		return false
	}
}

// cleanupSrc recursively cleans a single source node (not a list) — for
// SrcInorder/SrcAllotment it dispatches into the list-aware helpers below.
func cleanupSrc(s Source, emptiedAccounts map[string]bool) Source {
	switch s.Kind {
	case SrcInorder:
		s.Sources = cleanupSrcs(s.Sources, emptiedAccounts)
		return s
	case SrcOneof:
		// branches are alternatives, but the account-dedup map is threaded
		// through anyway: the pruning is generation-time shaping, and being
		// conservative here only trims coverage, never correctness
		s.Sources = cleanupSrcs(s.Sources, emptiedAccounts)
		return s
	case SrcCapped:
		cleaned := cleanupSrc(*s.Inner, emptiedAccounts)
		s.Inner = &cleaned
		return s
	case SrcAllotment:
		for i := range s.Clauses {
			s.Clauses[i].Source = cleanupSrc(s.Clauses[i].Source, emptiedAccounts)
		}
		if s.AllotmentRemaining != nil {
			cleaned := cleanupSrc(*s.AllotmentRemaining, emptiedAccounts)
			s.AllotmentRemaining = &cleaned
		}
		return s
	default: // SrcAccount, SrcAccountOverdraft — leaves, unchanged
		return s
	}
}

// cleanupSrcs walks a list of sibling sources (as found inside a SrcInorder), dropping
// an unbounded source unless it's the last element, and dropping any
// account-referencing source whose account was already used earlier
// anywhere in this statement's source tree (emptiedAccounts is threaded
// through the whole traversal, not scoped per-inorder-block).
func cleanupSrcs(srcs []Source, emptiedAccounts map[string]bool) []Source {
	if len(srcs) == 0 {
		return nil
	}

	head, rest := srcs[0], srcs[1:]

	if len(rest) > 0 && isSrcUnbounded(head) {
		return cleanupSrcs(rest, emptiedAccounts)
	}

	account, hasAccount := "", false
	switch {
	case head.Kind == SrcAccount:
		account, hasAccount = head.Account, true
	case head.Kind == SrcCapped && head.Inner != nil && head.Inner.Kind == SrcAccount:
		account, hasAccount = head.Inner.Account, true
	case head.Kind == SrcAccountOverdraft:
		account, hasAccount = head.Account, true
	}

	if hasAccount {
		if emptiedAccounts[account] {
			return cleanupSrcs(rest, emptiedAccounts)
		}
		emptiedAccounts[account] = true
	}

	cleanedHead := cleanupSrc(head, emptiedAccounts)
	return append([]Source{cleanedHead}, cleanupSrcs(rest, emptiedAccounts)...)
}

// removeEmptyAllotmentsSrc is a second, stateless pass pruning subtrees left
// empty by cleanupSrcs. Returns nil for "this subtree is now empty".
//
// SrcAllotment is fail-fast: if ANY clause becomes empty, the
// whole allotment is dropped (its portions can't be renormalized), unlike
// SrcInorder which just drops individually-empty children.
func removeEmptyAllotmentsSrc(s Source) *Source {
	switch s.Kind {
	case SrcInorder, SrcOneof:
		if len(s.Sources) == 0 {
			return nil
		}

		var survivors []Source
		for _, src := range s.Sources {
			if cleaned := removeEmptyAllotmentsSrc(src); cleaned != nil {
				survivors = append(survivors, *cleaned)
			}
		}
		if len(survivors) == 0 {
			return nil
		}
		s.Sources = survivors
		return &s

	case SrcCapped:
		inner := removeEmptyAllotmentsSrc(*s.Inner)
		if inner == nil {
			return nil
		}
		s.Inner = inner
		return &s

	case SrcAllotment:
		survivors := make([]SourceAllotmentClause, 0, len(s.Clauses))
		for _, clause := range s.Clauses {
			cleaned := removeEmptyAllotmentsSrc(clause.Source)
			if cleaned == nil {
				return nil
			}
			clause.Source = *cleaned
			survivors = append(survivors, clause)
		}
		s.Clauses = survivors
		if s.AllotmentRemaining != nil {
			cleaned := removeEmptyAllotmentsSrc(*s.AllotmentRemaining)
			if cleaned == nil {
				return nil
			}
			s.AllotmentRemaining = cleaned
		}
		return &s

	default: // SrcAccount, SrcAccountOverdraft — always survive
		return &s
	}
}
