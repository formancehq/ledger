package celrewrite

import (
	"maps"
	"regexp"
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The CEL variable is `log`: the actual mirror log entry, a sum type over the
// four rewritable variants (fill-gap is passed through, never seen by a rule).
// Each variant is a distinct CEL receiver type, so a helper that a variant's
// wire form cannot persist is a compile-time type error rather than a silent
// drop — e.g. setAccountMetadata exists only on `created`, and `deletedMetadata`
// exposes no metadata map at all. This mirrors raft_cmd.proto's MirrorLogEntry
// oneof and removes the invented `tx.type` discriminant of the earlier flat view.
//
// Exactly one variant pointer is non-nil, pinned from the source entry. Helpers
// return a copy-on-write view; a `log.withX(...)` wrapper lifts a transformed
// variant back into the entry (a variant helper alone cannot rebuild its parent
// log). The whole chain commits to the proto only after it succeeds.

// Kind discriminants, pinned on Log.kind from the source entry. Not exposed to
// CEL (the variant is observable via has(log.created) etc.).
const (
	KindCreated        = "created"
	KindReverted       = "reverted"
	KindSetMetadata    = "setMetadata"
	KindDeleteMetadata = "deleteMetadata"
)

// Posting is the CEL-visible view of a posting. Amount and asset are read-only
// (never written back); only source/destination are committed.
type Posting struct {
	Source      string `cel:"source"`
	Destination string `cel:"destination"`
	Amount      string `cel:"amount"`
	Asset       string `cel:"asset"`
}

// CreatedView is the view of a MirrorCreatedTransaction. It is the only variant
// carrying account metadata.
type CreatedView struct {
	Reference       string                       `cel:"reference"`
	Postings        []Posting                    `cel:"postings"`
	Metadata        map[string]string            `cel:"metadata"`
	AccountMetadata map[string]map[string]string `cel:"accountMetadata"`

	metadataTypes        map[string]commonpb.MetadataType
	accountMetadataTypes map[string]map[string]commonpb.MetadataType
}

// RevertedView is the view of a MirrorRevertedTransaction. `postings` are the
// reverse postings; there is no account_metadata field on the wire.
type RevertedView struct {
	Postings []Posting         `cel:"postings"`
	Metadata map[string]string `cel:"metadata"`

	metadataTypes map[string]commonpb.MetadataType
}

// SavedMetadataView is the view of a MirrorSavedMetadata (a setMetadata op).
type SavedMetadataView struct {
	Target   string            `cel:"target"`
	Metadata map[string]string `cel:"metadata"`

	metadataTypes  map[string]commonpb.MetadataType
	targetsAccount bool
}

// DeletedMetadataView is the view of a MirrorDeletedMetadata. It has no metadata
// map (only the key being deleted, read-only); its sole mutable field is the
// target address, rewritten via log.rewriteAddress / log.mapAddress.
type DeletedMetadataView struct {
	Target string `cel:"target"`
	Key    string `cel:"key"`

	targetsAccount bool
}

// Log is the CEL variable: exactly one variant pointer is non-nil.
type Log struct {
	Created         *CreatedView         `cel:"created"`
	Reverted        *RevertedView        `cel:"reverted"`
	SavedMetadata   *SavedMetadataView   `cel:"savedMetadata"`
	DeletedMetadata *DeletedMetadataView `cel:"deletedMetadata"`

	kind    string // pinned from the source entry; the committable variant
	dropped bool
}

// --- clone helpers ----------------------------------------------------------

func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	return maps.Clone(m)
}

func cloneTypeMap(m map[string]commonpb.MetadataType) map[string]commonpb.MetadataType {
	if m == nil {
		return nil
	}

	return maps.Clone(m)
}

func clonePostings(in []Posting) []Posting {
	if in == nil {
		return nil
	}

	out := make([]Posting, len(in))
	copy(out, in)

	return out
}

func cloneAccountMap(m map[string]map[string]string) map[string]map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]map[string]string, len(m))
	for k, v := range m {
		out[k] = maps.Clone(v)
	}

	return out
}

func cloneAccountTypeMap(m map[string]map[string]commonpb.MetadataType) map[string]map[string]commonpb.MetadataType {
	if m == nil {
		return nil
	}

	out := make(map[string]map[string]commonpb.MetadataType, len(m))
	for k, v := range m {
		out[k] = maps.Clone(v)
	}

	return out
}

func (c *CreatedView) clone() *CreatedView {
	return &CreatedView{
		Reference:            c.Reference,
		Postings:             clonePostings(c.Postings),
		Metadata:             cloneStringMap(c.Metadata),
		AccountMetadata:      cloneAccountMap(c.AccountMetadata),
		metadataTypes:        cloneTypeMap(c.metadataTypes),
		accountMetadataTypes: cloneAccountTypeMap(c.accountMetadataTypes),
	}
}

func (r *RevertedView) clone() *RevertedView {
	return &RevertedView{
		Postings:      clonePostings(r.Postings),
		Metadata:      cloneStringMap(r.Metadata),
		metadataTypes: cloneTypeMap(r.metadataTypes),
	}
}

func (s *SavedMetadataView) clone() *SavedMetadataView {
	return &SavedMetadataView{
		Target:         s.Target,
		Metadata:       cloneStringMap(s.Metadata),
		metadataTypes:  cloneTypeMap(s.metadataTypes),
		targetsAccount: s.targetsAccount,
	}
}

func (l *Log) clone() *Log {
	return &Log{
		Created:         l.Created,
		Reverted:        l.Reverted,
		SavedMetadata:   l.SavedMetadata,
		DeletedMetadata: l.DeletedMetadata,
		kind:            l.kind,
		dropped:         l.dropped,
	}
}

// --- shared metadata mutation (operates on an already-cloned view's maps) ----

// setMetadataEntry writes key=value with an optional declared type, allocating
// the maps if needed. An untyped write clears any previous type so the type
// sidecar never drifts from the value map (an untyped overwrite reverts the key
// to the default string type).
func setMetadataEntry(md map[string]string, mt map[string]commonpb.MetadataType, key, value string, t commonpb.MetadataType, typed bool) (map[string]string, map[string]commonpb.MetadataType) {
	if md == nil {
		md = map[string]string{}
	}

	md[key] = value

	if typed {
		if mt == nil {
			mt = map[string]commonpb.MetadataType{}
		}

		mt[key] = t
	} else {
		delete(mt, key)
	}

	return md, mt
}

func deleteMetadataEntry(md map[string]string, mt map[string]commonpb.MetadataType, key string) (map[string]string, map[string]commonpb.MetadataType) {
	delete(md, key)
	delete(mt, key)

	return md, mt
}

// --- account metadata (created only) ----------------------------------------

func (c *CreatedView) setAccountMetadata(account, key, value string, t commonpb.MetadataType, typed bool) {
	if c.AccountMetadata == nil {
		c.AccountMetadata = map[string]map[string]string{}
	}

	if c.AccountMetadata[account] == nil {
		c.AccountMetadata[account] = map[string]string{}
	}

	c.AccountMetadata[account][key] = value

	if !typed {
		c.clearAccountMetadataType(account, key)

		return
	}

	if c.accountMetadataTypes == nil {
		c.accountMetadataTypes = map[string]map[string]commonpb.MetadataType{}
	}

	if c.accountMetadataTypes[account] == nil {
		c.accountMetadataTypes[account] = map[string]commonpb.MetadataType{}
	}

	c.accountMetadataTypes[account][key] = t
}

func (c *CreatedView) deleteAccountMetadata(account, key string) {
	if inner := c.AccountMetadata[account]; inner != nil {
		delete(inner, key)
		if len(inner) == 0 {
			delete(c.AccountMetadata, account)
		}
	}

	c.clearAccountMetadataType(account, key)
}

func (c *CreatedView) clearAccountMetadataType(account, key string) {
	inner := c.accountMetadataTypes[account]
	if inner == nil {
		return
	}

	delete(inner, key)

	if len(inner) == 0 {
		delete(c.accountMetadataTypes, account)
	}
}

// --- address gather / scatter (variant-aware) -------------------------------

// orderedAddresses returns the active variant's account addresses in a stable
// order: each posting's source then destination, the account target (when the
// entry targets an account), then account-metadata keys sorted. withAddresses
// consumes the list in exactly this order.
func (l *Log) orderedAddresses() []string {
	switch {
	case l.Created != nil:
		c := l.Created
		acctKeys := sortedStringKeys(c.AccountMetadata)
		out := make([]string, 0, 2*len(c.Postings)+len(acctKeys))
		for _, p := range c.Postings {
			out = append(out, p.Source, p.Destination)
		}

		return append(out, acctKeys...)

	case l.Reverted != nil:
		r := l.Reverted
		out := make([]string, 0, 2*len(r.Postings))
		for _, p := range r.Postings {
			out = append(out, p.Source, p.Destination)
		}

		return out

	case l.SavedMetadata != nil && l.SavedMetadata.targetsAccount:
		return []string{l.SavedMetadata.Target}

	case l.DeletedMetadata != nil && l.DeletedMetadata.targetsAccount:
		return []string{l.DeletedMetadata.Target}
	}

	return nil
}

// withAddresses rebuilds the active variant with the given addresses, consumed
// in the exact order orderedAddresses produced them. It returns an error message
// (as a string) on a count mismatch; the caller wraps it as a CEL error.
func (l *Log) withAddresses(addrs []string) (*Log, string) {
	want := len(l.orderedAddresses())
	if len(addrs) != want {
		return nil, "expected the same number of addresses"
	}

	nl := l.clone()

	switch {
	case nl.Created != nil:
		c := nl.Created.clone()
		i := 0
		for p := range c.Postings {
			c.Postings[p].Source = addrs[i]
			c.Postings[p].Destination = addrs[i+1]
			i += 2
		}

		oldKeys := sortedStringKeys(c.AccountMetadata)
		newByOld := make(map[string]string, len(oldKeys))
		for j, old := range oldKeys {
			newByOld[old] = addrs[i+j]
		}

		c.AccountMetadata, c.accountMetadataTypes = remapAccountKeys(c.AccountMetadata, c.accountMetadataTypes, func(a string) string { return newByOld[a] })
		nl.Created = c

	case nl.Reverted != nil:
		r := nl.Reverted.clone()
		i := 0
		for p := range r.Postings {
			r.Postings[p].Source = addrs[i]
			r.Postings[p].Destination = addrs[i+1]
			i += 2
		}

		nl.Reverted = r

	case nl.SavedMetadata != nil && nl.SavedMetadata.targetsAccount:
		s := nl.SavedMetadata.clone()
		s.Target = addrs[0]
		nl.SavedMetadata = s

	case nl.DeletedMetadata != nil && nl.DeletedMetadata.targetsAccount:
		d := &DeletedMetadataView{Target: addrs[0], Key: nl.DeletedMetadata.Key, targetsAccount: true}
		nl.DeletedMetadata = d
	}

	return nl, ""
}

// rewriteAddresses applies re/replacement to every address of the active
// variant, re-keying created account metadata (and its type sidecar) with a
// deterministic sorted-order, last-writer-wins merge on collision.
func (l *Log) rewriteAddresses(re *regexp.Regexp, replacement string) *Log {
	nl := l.clone()

	switch {
	case nl.Created != nil:
		c := nl.Created.clone()
		for i := range c.Postings {
			c.Postings[i].Source = re.ReplaceAllString(c.Postings[i].Source, replacement)
			c.Postings[i].Destination = re.ReplaceAllString(c.Postings[i].Destination, replacement)
		}

		c.AccountMetadata, c.accountMetadataTypes = remapAccountKeys(c.AccountMetadata, c.accountMetadataTypes, func(a string) string {
			return re.ReplaceAllString(a, replacement)
		})
		nl.Created = c

	case nl.Reverted != nil:
		r := nl.Reverted.clone()
		for i := range r.Postings {
			r.Postings[i].Source = re.ReplaceAllString(r.Postings[i].Source, replacement)
			r.Postings[i].Destination = re.ReplaceAllString(r.Postings[i].Destination, replacement)
		}

		nl.Reverted = r

	case nl.SavedMetadata != nil && nl.SavedMetadata.targetsAccount:
		s := nl.SavedMetadata.clone()
		s.Target = re.ReplaceAllString(s.Target, replacement)
		nl.SavedMetadata = s

	case nl.DeletedMetadata != nil && nl.DeletedMetadata.targetsAccount:
		d := &DeletedMetadataView{
			Target:         re.ReplaceAllString(nl.DeletedMetadata.Target, replacement),
			Key:            nl.DeletedMetadata.Key,
			targetsAccount: true,
		}
		nl.DeletedMetadata = d
	}

	return nl
}

// remapAccountKeys rebuilds the account value + type maps under new keys given by
// keyFn, iterating source accounts in sorted order so a collision's last writer
// wins deterministically. Crucially, value and type move in lockstep per
// (account, metadata-key): the winning value's source decides the type, and an
// untyped winner clears any type an earlier colliding source set for that key.
// Re-keying the two maps independently would let the type sidecar drift from the
// value (an untyped winner inheriting a losing source's declared type), which
// commit would then coerce to a wrong (null) MetadataValue.
func remapAccountKeys(am map[string]map[string]string, at map[string]map[string]commonpb.MetadataType, keyFn func(string) string) (map[string]map[string]string, map[string]map[string]commonpb.MetadataType) {
	if len(am) == 0 {
		return am, at
	}

	nam := make(map[string]map[string]string, len(am))
	nat := map[string]map[string]commonpb.MetadataType{}

	for _, old := range sortedStringKeys(am) {
		neu := keyFn(old)
		if nam[neu] == nil {
			nam[neu] = map[string]string{}
		}

		srcTypes := at[old]
		for mk, mv := range am[old] {
			nam[neu][mk] = mv

			if t, typed := srcTypes[mk]; typed {
				if nat[neu] == nil {
					nat[neu] = map[string]commonpb.MetadataType{}
				}

				nat[neu][mk] = t
			} else if nat[neu] != nil {
				delete(nat[neu], mk)
			}
		}
	}

	for k, m := range nat {
		if len(m) == 0 {
			delete(nat, k)
		}
	}

	if len(nat) == 0 {
		nat = nil
	}

	return nam, nat
}

func sortedStringKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func sortStrings(s []string) { sort.Strings(s) }
