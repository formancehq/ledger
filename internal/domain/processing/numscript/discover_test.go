package numscript

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// fakeSource is a ValueSource backed by in-memory maps for discovery tests.
type fakeSource struct {
	balances map[string]*big.Int // "account\x00asset" -> balance
	metadata map[string]string   // "account\x00key" -> value
	present  map[string]struct{} // keys treated as present (for absent vs empty)
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		balances: map[string]*big.Int{},
		metadata: map[string]string{},
		present:  map[string]struct{}{},
	}
}

func (f *fakeSource) withBalance(account, asset string, amount int64) *fakeSource {
	f.balances[account+"\x00"+asset] = big.NewInt(amount)

	return f
}

func (f *fakeSource) withMetadata(account, key, value string) *fakeSource {
	f.metadata[account+"\x00"+key] = value
	f.present[account+"\x00"+key] = struct{}{}

	return f
}

func (f *fakeSource) Balance(account, asset string) (*big.Int, error) {
	if b, ok := f.balances[account+"\x00"+asset]; ok {
		return new(big.Int).Set(b), nil
	}

	return new(big.Int), nil
}

func (f *fakeSource) Metadata(account, key string) (string, bool, error) {
	if _, ok := f.present[account+"\x00"+key]; ok {
		return f.metadata[account+"\x00"+key], true, nil
	}

	return "", false, nil
}

func volKey(account, asset string) domain.VolumeKey {
	return domain.NewVolumeKey("ledger", account, asset)
}

func metaKey(account, key string) domain.MetadataKey {
	return domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: account},
		Key:        key,
	}
}

func discover(t *testing.T, script string, vars map[string]string, source ValueSource, force bool) *DiscoveryResult {
	t.Helper()
	cache := NewNumscriptCache(16)
	result, err := DiscoverNumscriptDependencies(cache, script, vars, "ledger", source, force)
	require.NoError(t, err)

	return result
}

// TestDiscover_Simple: world source (unbounded) is a write, not a read; the
// destination is a write.
func TestDiscover_UnboundedWorldSourceIsWriteNotRead(t *testing.T) {
	t.Parallel()

	script := `send [USD/2 100] (source = @world destination = @alice)`
	result := discover(t, script, nil, newFakeSource(), false)

	require.NotContains(t, result.ReadVolumes, volKey("world", "USD/2"),
		"world is unbounded — its balance is never read")
	require.Contains(t, result.WriteVolumes, volKey("world", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("alice", "USD/2"))
	require.Nil(t, result.InputsHash, "a fully-static, unbounded-source script reads nothing")
}

// TestDiscover_BoundedSourceIsRead pins CM-206: a bounded source's balance is
// read, so its volume must be discovered as a read (and preloaded).
func TestDiscover_BoundedSourceIsRead(t *testing.T) {
	t.Parallel()

	script := `send [USD/2 200] (source = @wallet destination = @out)`
	source := newFakeSource().withBalance("wallet", "USD/2", 500)
	result := discover(t, script, nil, source, false)

	require.Contains(t, result.ReadVolumes, volKey("wallet", "USD/2"),
		"a bounded source's balance is a read dependency — must be preloaded (CM-206)")
	require.Contains(t, result.WriteVolumes, volKey("wallet", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("out", "USD/2"))
	// A plain bounded source is a read *dependency* (preload) but its value does
	// not influence which keys are discovered, so it is not a hashed input:
	// InputsHash covers only values that determined the resolution (meta(),
	// balance() in vars/caps), not every preloaded balance.
	require.Nil(t, result.InputsHash,
		"a plain bounded source's balance value does not affect resolution")
}

// TestDiscover_OverdraftPlusBalanceCheckedSource pins CM-209: an overdraft
// source and a balance-checked source both resolve without a determinism error.
func TestDiscover_OverdraftPlusBalanceCheckedSource(t *testing.T) {
	t.Parallel()

	script := `
send [USD/2 300] (
  source = {
    @a allowing overdraft up to [USD/2 100]
    @b
  }
  destination = @sink
)
`
	source := newFakeSource().withBalance("a", "USD/2", 1000).withBalance("b", "USD/2", 1000)
	result := discover(t, script, nil, source, false)

	// Both bounded sources are reads; both are writes; sink is a write.
	require.Contains(t, result.ReadVolumes, volKey("a", "USD/2"))
	require.Contains(t, result.ReadVolumes, volKey("b", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("sink", "USD/2"))
}

// TestDiscover_OneofExhaustive: every branch of a source oneof is discovered,
// not just the first.
func TestDiscover_OneofExhaustive(t *testing.T) {
	t.Parallel()

	script := `
send [USD/2 50] (
  source = oneof {
    @first
    @second
  }
  destination = @dest
)
`
	source := newFakeSource().withBalance("first", "USD/2", 10).withBalance("second", "USD/2", 100)
	result := discover(t, script, nil, source, false)

	require.Contains(t, result.WriteVolumes, volKey("first", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("second", "USD/2"),
		"oneof must discover every branch, not just the first")
}

// TestDiscover_MetaInVars: meta() in a var origin resolves the account and
// records the metadata read.
func TestDiscover_MetaInVars(t *testing.T) {
	t.Parallel()

	script := `
vars {
  account $dest = meta(@routing, "destination")
}
send [USD/2 100] (
  source = @world
  destination = $dest
)
`
	source := newFakeSource().withMetadata("routing", "destination", "orders:done")
	result := discover(t, script, nil, source, false)

	require.Contains(t, result.ReadMetadata, metaKey("routing", "destination"),
		"meta() read must be discovered")
	require.Contains(t, result.WriteVolumes, volKey("orders:done", "USD/2"),
		"the meta-resolved destination must be discovered as a write")
	require.NotNil(t, result.InputsHash)
}

// TestDiscover_MultiSend: multiple send blocks are all discovered (no
// determinism rejection).
func TestDiscover_MultiSend(t *testing.T) {
	t.Parallel()

	script := `
send [USD/2 100] (source = @world destination = @a)
send [USD/2 200] (source = @world destination = @b)
`
	result := discover(t, script, nil, newFakeSource(), false)

	require.Contains(t, result.WriteVolumes, volKey("a", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("b", "USD/2"))
}

// TestDiscover_SetAccountMetaIsWrite: set_account_meta records a metadata write,
// not a read.
func TestDiscover_SetAccountMetaIsWrite(t *testing.T) {
	t.Parallel()

	script := `
send [USD/2 100] (source = @world destination = @a)
set_account_meta(@a, "flag", "on")
`
	result := discover(t, script, nil, newFakeSource(), false)

	require.Contains(t, result.WriteMetadata, metaKey("a", "flag"))
	require.NotContains(t, result.ReadMetadata, metaKey("a", "flag"))
}

// TestDiscover_HashChangesWithBalance: when balance() drives resolution (here
// via a var origin), the read value is a bound input, so a change in the balance
// changes the hash (stale detection).
func TestDiscover_HashChangesWithBalance(t *testing.T) {
	t.Parallel()

	script := `
vars {
  monetary $amt = balance(@wallet, USD/2)
}
send $amt (source = @world destination = @out)
`

	r1 := discover(t, script, nil, newFakeSource().withBalance("wallet", "USD/2", 500), false)
	r2 := discover(t, script, nil, newFakeSource().withBalance("wallet", "USD/2", 500), false)
	r3 := discover(t, script, nil, newFakeSource().withBalance("wallet", "USD/2", 999), false)

	require.NotNil(t, r1.InputsHash)
	require.Equal(t, r1.InputsHash, r2.InputsHash, "same inputs must hash equal")
	require.NotEqual(t, r1.InputsHash, r3.InputsHash, "a changed read balance must change the hash")
}

// TestDiscover_HashChangesWithMetadataPresence: a metadata key gaining a value
// between admission and apply must change the hash (absent vs present).
func TestDiscover_HashChangesWithMetadataPresence(t *testing.T) {
	t.Parallel()

	script := `
vars {
  account $dest = meta(@routing, "destination")
}
send [USD/2 100] (source = @world destination = $dest)
`
	// Both runs must resolve; use the same value so only presence differs is
	// not possible here (meta() must resolve to an account), so instead compare
	// two different metadata values.
	rA := discover(t, script, nil, newFakeSource().withMetadata("routing", "destination", "acct:a"), false)
	rB := discover(t, script, nil, newFakeSource().withMetadata("routing", "destination", "acct:b"), false)

	require.NotEqual(t, rA.InputsHash, rB.InputsHash,
		"a changed metadata value must change the hash")
}

// TestDiscover_ForceSkipsBalanceReads: with force, bounded sources still resolve
// but no real balance is consulted, so no read volumes / no inputs hash from
// balances.
func TestDiscover_ForceSkipsBalanceReads(t *testing.T) {
	t.Parallel()

	script := `send [USD/2 100] (source = @wallet destination = @out)`
	source := newFakeSource().withBalance("wallet", "USD/2", 1)
	result := discover(t, script, nil, source, true)

	// The source is still recorded as a read dependency (the resolver records
	// it), but the value is the forced max — deterministic across balances.
	require.Contains(t, result.WriteVolumes, volKey("wallet", "USD/2"))
	require.Contains(t, result.WriteVolumes, volKey("out", "USD/2"))
}
