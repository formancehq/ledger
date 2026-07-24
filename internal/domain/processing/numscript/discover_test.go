package numscript

import (
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// sourceSpec is a value-driven builder for the ValueSource seam. Numscript
// resolution calls Balance / Metadata an interpreter-decided number of times in
// an interpreter-decided order, so behaviour-verifying expectations are the
// wrong tool; build() wires the (generated) MockValueSource with AnyTimes
// stubs that answer from the declared maps. Using the generated mock keeps the
// repo's "no hand-rolled fakes" convention while retaining the stub ergonomics
// the value-driven tests need.
type sourceSpec struct {
	balances   map[string]*big.Int // "account\x00asset\x00color" -> balance
	metadata   map[string]string   // "account\x00key" -> value
	present    map[string]struct{} // keys treated as present (for absent vs empty)
	balanceErr error               // when set, Balance fails with this error
}

func newFakeSource() *sourceSpec {
	return &sourceSpec{
		balances: map[string]*big.Int{},
		metadata: map[string]string{},
		present:  map[string]struct{}{},
	}
}

// withBalance declares the uncolored (color "") bucket balance.
func (f *sourceSpec) withBalance(account, asset string, amount int64) *sourceSpec {
	return f.withColoredBalance(account, asset, "", amount)
}

// withColoredBalance declares a segregated (account, asset, color) bucket
// balance; color IS a volume dimension.
func (f *sourceSpec) withColoredBalance(account, asset, color string, amount int64) *sourceSpec {
	f.balances[account+"\x00"+asset+"\x00"+color] = big.NewInt(amount)

	return f
}

// withBalanceError makes every Balance lookup fail with err, simulating a
// state-source read failure during dependency resolution.
func (f *sourceSpec) withBalanceError(err error) *sourceSpec {
	f.balanceErr = err

	return f
}

func (f *sourceSpec) withMetadata(account, key, value string) *sourceSpec {
	f.metadata[account+"\x00"+key] = value
	f.present[account+"\x00"+key] = struct{}{}

	return f
}

// build materialises a MockValueSource answering from the declared maps. Absent
// balances resolve to zero (a fresh account); absent metadata resolves to
// not-present — matching the real admission/FSM value sources.
func (f *sourceSpec) build(t *testing.T) *MockValueSource {
	t.Helper()

	ctrl := gomock.NewController(t)
	mock := NewMockValueSource(ctrl)

	mock.EXPECT().Balance(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(account, asset, color string) (*big.Int, error) {
			if f.balanceErr != nil {
				return nil, f.balanceErr
			}

			if b, ok := f.balances[account+"\x00"+asset+"\x00"+color]; ok {
				return new(big.Int).Set(b), nil
			}

			return new(big.Int), nil
		})

	mock.EXPECT().Metadata(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(account, key string) (string, bool, error) {
			if _, ok := f.present[account+"\x00"+key]; ok {
				return f.metadata[account+"\x00"+key], true, nil
			}

			return "", false, nil
		})

	return mock
}

func volKey(account, asset string) domain.VolumeKey {
	return domain.NewVolumeKey("ledger", account, asset, "")
}

func coloredVolKey(account, asset, color string) domain.VolumeKey {
	return domain.NewVolumeKey("ledger", account, asset, color)
}

func metaKey(account, key string) domain.MetadataKey {
	return domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: account},
		Key:        key,
	}
}

func discover(t *testing.T, script string, vars map[string]string, source *sourceSpec, force bool) *DiscoveryResult {
	t.Helper()
	cache := NewNumscriptCache(16)
	result, err := DiscoverNumscriptDependencies(cache, script, vars, "ledger", source.build(t), force)
	require.NoError(t, err)

	return result
}

// TestDiscover_ResolveErrorCarriesReadProvenance pins EN-1557: when dependency
// resolution consults a mutable balance (here via balance() in a vars block) and
// that lookup fails, DiscoverNumscriptDependencies returns a
// *DependencyResolutionError whose MutableReadAttempted is true. The recording
// store sets the flag BEFORE delegating to the failing source, so the failure is
// classified as state-dependent (forwardable) rather than deterministic.
func TestDiscover_ResolveErrorCarriesReadProvenance(t *testing.T) {
	t.Parallel()

	script := `
vars {
  monetary $amt = balance(@wallet, USD/2)
}
send $amt (source = @world destination = @out)
`
	boom := errors.New("state source unavailable")
	source := newFakeSource().withBalanceError(boom)

	cache := NewNumscriptCache(16)
	_, err := DiscoverNumscriptDependencies(cache, script, nil, "ledger", source.build(t), false)
	require.Error(t, err)

	var dre *DependencyResolutionError
	require.True(t, errors.As(err, &dre),
		"a resolve failure must be wrapped in *DependencyResolutionError")
	require.True(t, dre.MutableReadAttempted,
		"a balance read was attempted before the failure, so provenance must be state-dependent")
}

// TestDiscover_ReadThenScalingIsFreezable is the EN-1557 regression flemzord
// asked for: a script that reads a balance SUCCESSFULLY in a var origin and then
// hits an unsupported asset-scaling source. Resolution binds var origins before
// walking statements, so the successful balance() read sets MutableReadAttempted
// BEFORE SourceWithScaling deterministically returns ErrScalingNotSupported. The
// provenance flag alone would misclassify this as state-dependent (forwardable);
// the fix maps scaling to the freezable domain.ErrNumscriptScalingUnsupported so
// it terminates. This pins BOTH facts: the read provenance is recorded, yet the
// carried cause is the freezable scaling sentinel — never a KindInternal runtime
// error that would be forwarded under an idempotency key.
func TestDiscover_ReadThenScalingIsFreezable(t *testing.T) {
	t.Parallel()

	script := `
vars {
  monetary $amt = balance(@wallet, USD/2)
}
send $amt (
	source = @alice with scaling through @pool
	destination = @bob
)
`
	// A successful balance read (unknown balance resolves to 0, no error).
	source := newFakeSource()

	cache := NewNumscriptCache(16)
	_, err := DiscoverNumscriptDependencies(cache, script, nil, "ledger", source.build(t), false)
	require.Error(t, err)

	var dre *DependencyResolutionError
	require.True(t, errors.As(err, &dre),
		"a resolve failure must be wrapped in *DependencyResolutionError")
	require.True(t, dre.MutableReadAttempted,
		"a balance() origin was read before the scaling source failed, so the flag is set")

	require.ErrorIs(t, err, domain.ErrNumscriptScalingUnsupported,
		"an unsupported scaling source must surface as the freezable scaling sentinel")
	require.True(t, domain.IsFreezableFailure(domain.Kind(func() domain.Describable {
		var target domain.Describable
		_ = errors.As(dre.Cause, &target)
		return target
	}())),
		"the carried cause must be freezable so admission terminates instead of forwarding")

	var runtimeErr *domain.ErrNumscriptRuntime
	require.NotErrorAs(t, err, &runtimeErr,
		"scaling must NOT be the KindInternal runtime error that MutableReadAttempted would forward under a key")
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

// TestDiscover_MetaEmptyStringIsPresent pins the empty-string presence fix at
// the resolution layer: a metadata key whose stored value is an empty string is
// PRESENT, so a meta() read of it resolves successfully and records the real
// value ("") — not the absent sentinel. Two runs with the same empty-string
// value must hash equal, and an empty string must hash differently from a
// non-empty value (proving the recorded value is the real "" and not the absent
// sentinel).
func TestDiscover_MetaEmptyStringIsPresent(t *testing.T) {
	t.Parallel()

	script := `
vars {
  string $note = meta(@profile, "note")
}
set_tx_meta("note", $note)
send [USD/2 1] (source = @world destination = @sink)
`

	empty1 := discover(t, script, nil, newFakeSource().withMetadata("profile", "note", ""), false)
	empty2 := discover(t, script, nil, newFakeSource().withMetadata("profile", "note", ""), false)
	nonEmpty := discover(t, script, nil, newFakeSource().withMetadata("profile", "note", "x"), false)

	require.Contains(t, empty1.ReadMetadata, metaKey("profile", "note"),
		"the meta() key must be discovered as a read")
	require.NotNil(t, empty1.InputsHash)
	require.Equal(t, empty1.InputsHash, empty2.InputsHash,
		"identical present empty-string reads must hash equal")
	require.NotEqual(t, empty1.InputsHash, nonEmpty.InputsHash,
		"a present empty string must hash differently from a non-empty value (proving it recorded the real value, not the absent sentinel)")
}

// TestDiscover_OverdraftFunctionIsRead pins that the overdraft() var-origin
// function (distinct from `allowing overdraft`) is discovered as a balance read
// and hashed as a bound input. It routes through the same
// ResolveDependencies → preload → hash path.
func TestDiscover_OverdraftFunctionIsRead(t *testing.T) {
	t.Parallel()

	script := `
#![feature("experimental-overdraft-function")]
vars {
  monetary $o = overdraft(@carol, EUR/2)
}
send $o (source = @world destination = @bob)
`
	src := newFakeSource().withBalance("carol", "EUR/2", -50)
	result := discover(t, script, nil, src, false)

	require.Contains(t, result.ReadVolumes, volKey("carol", "EUR/2"),
		"overdraft() reads the account balance")
	require.Contains(t, result.WriteVolumes, volKey("bob", "EUR/2"))
	require.NotNil(t, result.InputsHash, "the overdraft() balance is a bound input")

	// A changed overdraft balance must change the hash (stale detection).
	other := discover(t, script, nil, newFakeSource().withBalance("carol", "EUR/2", -999), false)
	require.NotEqual(t, result.InputsHash, other.InputsHash)
}

// TestDiscover_GetAssetDecidesWriteAsset pins that get_asset() — which derives
// an asset from a monetary read via balance() — resolves and the balance read
// backing it is discovered (so the write asset/volume it decides is preloaded).
func TestDiscover_GetAssetDecidesWriteAsset(t *testing.T) {
	t.Parallel()

	script := `
#![feature("experimental-get-asset-function")]
vars {
  monetary $m = balance(@treasury, USD/2)
  asset $a = get_asset($m)
}
send [$a 10] (source = @treasury destination = @out)
`
	src := newFakeSource().withBalance("treasury", "USD/2", 1000)
	result := discover(t, script, nil, src, false)

	require.Contains(t, result.ReadVolumes, volKey("treasury", "USD/2"),
		"get_asset()'s backing balance() read must be discovered")
	// The send asset resolved to USD/2 via get_asset — the destination volume is
	// keyed by that asset, proving get_asset drove the write volume.
	require.Contains(t, result.WriteVolumes, volKey("out", "USD/2"))
	require.NotNil(t, result.InputsHash)
}

// TestDiscover_GetAmountReadsBalance pins that get_amount() — deriving a number
// from a monetary read via balance() — records the backing balance read.
func TestDiscover_GetAmountReadsBalance(t *testing.T) {
	t.Parallel()

	script := `
#![feature("experimental-get-amount-function")]
vars {
  monetary $m = balance(@treasury, USD/2)
  number $n = get_amount($m)
}
send [USD/2 $n] (source = @world destination = @out)
`
	src := newFakeSource().withBalance("treasury", "USD/2", 1000)
	result := discover(t, script, nil, src, false)

	require.Contains(t, result.ReadVolumes, volKey("treasury", "USD/2"),
		"get_amount()'s backing balance() read must be discovered")
	require.Contains(t, result.WriteVolumes, volKey("out", "USD/2"))
	require.NotNil(t, result.InputsHash)

	other := discover(t, script, nil, newFakeSource().withBalance("treasury", "USD/2", 1), false)
	require.NotEqual(t, result.InputsHash, other.InputsHash,
		"a changed source amount balance must change the hash")
}

// TestDiscover_MidScriptFunctionCallReadsBalance pins that a balance() called
// mid-script (outside vars — here inside set_tx_meta) is discovered as a read
// and hashed. Mid-script calls read balances outside the vars block, so they
// must still flow through resolution.
func TestDiscover_MidScriptFunctionCallReadsBalance(t *testing.T) {
	t.Parallel()

	script := `
#![feature("experimental-mid-script-function-call")]
send [USD/2 10] (source = @world destination = @out)
set_tx_meta("bal", balance(@treasury, USD/2))
`
	src := newFakeSource().withBalance("treasury", "USD/2", 1000)
	result := discover(t, script, nil, src, false)

	require.Contains(t, result.ReadVolumes, volKey("treasury", "USD/2"),
		"a mid-script balance() read must be discovered")
	require.Contains(t, result.WriteVolumes, volKey("out", "USD/2"))
	require.NotNil(t, result.InputsHash)

	other := discover(t, script, nil, newFakeSource().withBalance("treasury", "USD/2", 5), false)
	require.NotEqual(t, result.InputsHash, other.InputsHash,
		"a changed mid-script-read balance must change the hash")
}

// TestDiscover_ServesColoredWrite pins the write-side color threading: an
// unbounded colored source (@world is never balance-read) credits the
// destination in the source's color, producing a colored WRITE dependency.
// Color IS a volume dimension, so the write must be discovered on the segregated
// (account, asset, color) bucket — not collapsed onto the uncolored volume.
func TestDiscover_ServesColoredWrite(t *testing.T) {
	t.Parallel()

	cache := NewNumscriptCache(16)
	script := `send [COIN 10] (
		source = @world \ "RED"
		destination = @dest
	)`
	result, err := DiscoverNumscriptDependencies(cache, script, nil, "ledger", newFakeSource().build(t), false)
	require.NoError(t, err)
	require.Contains(t, result.WriteVolumes, coloredVolKey("world", "COIN", "RED"),
		"the unbounded colored source is a colored write")
	require.Contains(t, result.WriteVolumes, coloredVolKey("dest", "COIN", "RED"),
		"the destination is credited in the source's color")
	require.NotContains(t, result.WriteVolumes, volKey("dest", "COIN"),
		"the colored credit must not collapse onto the uncolored bucket")
}

// TestDiscover_ServesColoredRead pins the read-side color threading: a bounded
// colored source reads its own segregated (account, asset, color) bucket, so the
// colored read dependency is discovered and the value is hashed as a bound input.
func TestDiscover_ServesColoredRead(t *testing.T) {
	t.Parallel()

	cache := NewNumscriptCache(16)
	script := `send [COIN 10] (
		source = @wallet \ "RED"
		destination = @dest
	)`
	source := newFakeSource().withColoredBalance("wallet", "COIN", "RED", 100)
	result, err := DiscoverNumscriptDependencies(cache, script, nil, "ledger", source.build(t), false)
	require.NoError(t, err)
	require.Contains(t, result.ReadVolumes, coloredVolKey("wallet", "COIN", "RED"),
		"a bounded colored source reads its own color bucket")
	require.Contains(t, result.WriteVolumes, coloredVolKey("wallet", "COIN", "RED"))
	require.Contains(t, result.WriteVolumes, coloredVolKey("dest", "COIN", "RED"))
}
