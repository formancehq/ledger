package numscript

import (
	"context"
	"encoding/binary"
	"math/big"
	"sort"

	"github.com/zeebo/blake3"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// MaxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account, bypassing
// balance checks in Numscript execution.
var MaxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -destination=valuesource_generated_test.go -typed -package=numscript . ValueSource
//go:generate mockgen -write_source_comment=false -write_package_comment=false -destination=numscriptmock/valuesource_generated.go -typed -package=numscriptmock . ValueSource

// ValueSource is the minimal read surface the Numscript dependency resolver and
// the force-free execution path need: a per-(account, asset) balance and a
// per-(account, key) metadata value. It abstracts over where the values come
// from so the same numscript.Store adapter serves both:
//
//   - admission time: values read from a Pebble snapshot;
//   - FSM apply time: values read through the coverage-gated Scope, which never
//     touches Pebble (invariant #3) and only admits keys admission declared in
//     the preload set (invariant #9).
//
// Balance returns the (Input - Output) balance for (account, asset); a fresh
// account with no volume must return a zero balance, not an error. Metadata
// returns the verbatim stored value and whether it was present.
type ValueSource interface {
	Balance(account, asset string) (*big.Int, error)
	Metadata(account, key string) (value string, present bool, err error)
}

// Store adapts a ValueSource to the numscript library's Store interface. The
// upstream resolver and interpreter call GetBalances / GetAccountsMetadata with
// batched, slice-shaped queries; this translates each row through the
// ValueSource.
//
// Colors and scopes are not modelled by Ledger volumes/metadata, so the
// Color/Scope fields of the query are echoed back on the returned rows (so the
// interpreter's key matching lines up) but do not participate in the lookup.
//
// When force is set, GetBalances short-circuits to MaxForceBalance for every
// queried (account, asset), bypassing balance checks — this mirrors the
// transaction's force flag on the execution path.
type Store struct {
	source ValueSource
	force  bool
}

// NewStore builds a numscript Store over a ValueSource. force=true returns
// unlimited balances (force mode); force=false reads real balances.
func NewStore(source ValueSource, force bool) *Store {
	return &Store{source: source, force: force}
}

func (s *Store) GetBalances(_ context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	out := make(numscriptlib.Balances, 0, len(query))
	for _, item := range query {
		// Ledger volumes have no color/scope dimension: every color/scope view of
		// (account, asset) resolves to the SAME underlying volume. Answering a
		// qualified query would hand the caller a full-balance view per color and
		// let one script spend the same funds once per color (EN-1406 P1-2). The
		// lookup already ignores Color/Scope, so reject the query outright rather
		// than silently serving an unsound, double-counted view.
		if item.Color != "" || item.Scope != "" {
			return nil, domain.ErrColoredBalanceUnsupported
		}

		var balance *big.Int
		if s.force {
			balance = new(big.Int).Set(MaxForceBalance)
		} else {
			b, err := s.source.Balance(item.Account, item.Asset)
			if err != nil {
				return nil, err
			}

			if b == nil {
				b = new(big.Int)
			}

			balance = b
		}

		out = append(out, numscriptlib.BalanceRow{
			Account: item.Account,
			Asset:   item.Asset,
			Color:   item.Color,
			Scope:   item.Scope,
			Amount:  balance,
		})
	}

	return out, nil
}

func (s *Store) GetAccountsMetadata(_ context.Context, query numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	var out numscriptlib.AccountsMetadata
	for _, item := range query {
		// Ledger account metadata is keyed by (ledger, account, key) with no scope
		// dimension, so a scope-qualified metadata read would collapse to the same
		// entry as the unscoped one — reject it for the same reason as colored
		// balances (EN-1406 P1-2).
		if item.Scope != "" {
			return nil, domain.ErrColoredBalanceUnsupported
		}

		for _, key := range item.Keys {
			value, present, err := s.source.Metadata(item.Account, key)
			if err != nil {
				return nil, err
			}

			if !present {
				continue
			}

			out = append(out, numscriptlib.AccountMetadataRow{
				Account: item.Account,
				Scope:   item.Scope,
				Key:     key,
				Value:   value,
			})
		}
	}

	return out, nil
}

// RecordingStore wraps a numscript.Store and records every value the resolver
// reads through it, keyed by query, so the exact inputs that determined the
// resolved dependency set can be bound to the order via Hash().
//
// The FSM re-resolves against preloaded values, wraps its own RecordingStore,
// and compares hashes: a mismatch means an input value changed between
// admission and apply, so the preload set may be wrong (see
// domain.ErrStaleInputsResolution / EN-1406).
type RecordingStore struct {
	inner numscriptlib.Store

	balanceRecords  map[string]string // "account\x00asset\x00color\x00scope" -> amount (decimal)
	metadataRecords map[string]string // "account\x00scope\x00key" -> value or absent sentinel
}

// NewRecordingStore wraps inner so that every balance/metadata it returns is
// recorded for later hashing.
func NewRecordingStore(inner numscriptlib.Store) *RecordingStore {
	return &RecordingStore{
		inner:           inner,
		balanceRecords:  map[string]string{},
		metadataRecords: map[string]string{},
	}
}

func (s *RecordingStore) GetBalances(ctx context.Context, query numscriptlib.BalanceQuery) (numscriptlib.Balances, error) {
	rows, err := s.inner.GetBalances(ctx, query)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		amount := "0"
		if row.Amount != nil {
			amount = row.Amount.String()
		}

		s.balanceRecords[balanceRecordKey(row.Account, row.Asset, row.Color, row.Scope)] = amount
	}

	return rows, nil
}

// metadataAbsentSentinel marks a metadata key the resolver asked for but the
// store had no value for. Absence is part of the resolution input: a key that
// gains a value between admission and apply must invalidate the resolution, so
// an admission-time "absent" must hash differently from an apply-time value.
// The leading NUL cannot collide with a real stored value (metadata values are
// validated NUL-free before storage).
const metadataAbsentSentinel = "\x00absent"

func (s *RecordingStore) GetAccountsMetadata(ctx context.Context, query numscriptlib.MetadataQuery) (numscriptlib.AccountsMetadata, error) {
	rows, err := s.inner.GetAccountsMetadata(ctx, query)
	if err != nil {
		return nil, err
	}

	present := make(map[string]string, len(rows))
	for _, row := range rows {
		present[metadataRecordKey(row.Account, row.Scope, row.Key)] = row.Value
	}

	for _, item := range query {
		for _, key := range item.Keys {
			k := metadataRecordKey(item.Account, item.Scope, key)
			if value, ok := present[k]; ok {
				s.metadataRecords[k] = value
			} else {
				s.metadataRecords[k] = metadataAbsentSentinel
			}
		}
	}

	return rows, nil
}

func balanceRecordKey(account, asset, color, scope string) string {
	return account + "\x00" + asset + "\x00" + color + "\x00" + scope
}

func metadataRecordKey(account, scope, key string) string {
	return account + "\x00" + scope + "\x00" + key
}

// ReadNothing reports whether the resolution consulted no balance or metadata
// input at all. Such orders carry no stale-inputs hash, so the FSM skips the
// comparison entirely.
func (s *RecordingStore) ReadNothing() bool {
	return len(s.balanceRecords) == 0 && len(s.metadataRecords) == 0
}

// Hash returns a deterministic BLAKE3 digest over the recorded balance and
// metadata reads. Records are sorted so the digest is independent of the order
// the resolver happened to query in. When nothing was read (ReadNothing), Hash
// returns nil: there are no inputs to bind, and the FSM treats a nil/empty
// stored hash as "no stale check needed".
func (s *RecordingStore) Hash() []byte {
	if s.ReadNothing() {
		return nil
	}

	h := blake3.New()

	// Length-delimited encoding: every field is a uvarint byte-length followed by
	// its raw bytes, and every section is prefixed by its record count. Plain
	// `key=value\n` framing was ambiguous — metadata values are arbitrary client
	// bytes (only NUL is rejected; `=` and `\n` are valid), so a crafted value
	// could make a *changed* input set serialize to the same stream and evade
	// stale detection. Length + count prefixes make the encoding injective, so
	// distinct record sets always hash distinctly.
	writeField := func(b string) {
		var lenBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
		_, _ = h.Write(lenBuf[:n])
		_, _ = h.WriteString(b)
	}
	writeSection := func(label string, records map[string]string) {
		writeField(label)
		keys := make([]string, 0, len(records))
		for k := range records {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var cntBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(cntBuf[:], uint64(len(keys)))
		_, _ = h.Write(cntBuf[:n])

		for _, k := range keys {
			writeField(k)
			writeField(records[k])
		}
	}

	writeSection("balances", s.balanceRecords)
	writeSection("metadata", s.metadataRecords)

	return h.Sum(nil)
}
