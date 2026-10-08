package oracle

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/numscriptgen"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func scriptReq(ledger, plain string, vars map[string]string, force bool) *servicepb.Request {
	return createReq(ledger, &servicepb.CreateTransactionPayload{
		Script: &commonpb.Script{Plain: plain, Vars: vars},
		Force:  force,
	})
}

func referenceReq(ledger, name, version string, vars map[string]string) *servicepb.Request {
	return createReq(ledger, &servicepb.CreateTransactionPayload{
		ScriptReference: &servicepb.ScriptReference{Name: name, Version: version, Vars: vars},
	})
}

func createReq(ledger string, ct *servicepb.CreateTransactionPayload) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: ct}},
	}}}
}

func saveReq(ledger, name, version, content string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{
		Ledger: ledger, Name: name, Version: version, Content: content,
	}}}
}

const sendTen = `send [USD 10] (
	source = @a:1
	destination = @b:1
)`

func TestScript_FundsFloorAndForce(t *testing.T) {
	t.Parallel()

	unfunded := NewGlobalState().Apply(bulkOf(scriptReq("L", sendTen, nil, false)))
	require.False(t, unfunded.OK)
	require.Equal(t, domain.ErrReasonInsufficientFunds, unfunded.Reason)

	forced := NewGlobalState().Apply(bulkOf(scriptReq("L", sendTen, nil, true)))
	require.True(t, forced.OK)
	require.Len(t, forced.Orders[0].Postings, 1)

	funded := NewGlobalState().Apply(bulkOf(
		oracletest.TxReqL("L", "world", "a:1", "USD", 10),
		scriptReq("L", sendTen, nil, false),
	))
	require.True(t, funded.OK)

	ls := funded.State.Ledger("L")
	require.Equal(t, "10", dec(ls.vol(VolumeKey{"b:1", "USD", ""}).Input))
	require.True(t, ls.txs.Get(1).scripted)
}

// Under force every balance reads as 2^256, so a balance() origin sent whole
// is one past the widest amount a posting can carry.
func TestScript_ForcedBalanceOverflowsPosting(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(scriptReq("L", `vars {
	monetary $all = balance(@a:1, USD)
}
send $all (
	source = @a:1
	destination = @b:1
)`, nil, true)))
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonNumscriptExecutionError, res.Reason)
}

func TestScript_MetadataMergeCallerWins(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(createReq("L", &servicepb.CreateTransactionPayload{
		Script: &commonpb.Script{Plain: `send [USD 1] (
	source = @world
	destination = @b:1
)
set_tx_meta("k", "script")
set_tx_meta("only", "script")
set_account_meta(@b:1, "k", "script")`},
		Metadata: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("caller")},
		AccountMetadata: map[string]*commonpb.MetadataMap{
			"b:1": {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("caller")}},
		},
	})))
	require.True(t, res.OK)

	tx := res.State.Ledger("L").txs.Get(0)
	require.Equal(t, "s:caller", MetaValueString(tx.metadata["k"]))
	require.Equal(t, "s:script", MetaValueString(tx.metadata["only"]))
	require.Equal(t, "s:caller", MetaValueString(res.Orders[0].AccountMetadata["b:1"].GetValues()["k"]))
}

func TestScript_EmptyResultRejected(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(scriptReq("L", `set_tx_meta("k", "v")`, nil, false)))
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonValidation, res.Reason)
}

func TestScript_AdmissionRejections(t *testing.T) {
	t.Parallel()

	parse := NewGlobalState().Apply(bulkOf(scriptReq("L", "send [USD 1] (", nil, false)))
	require.Equal(t, domain.ErrReasonNumscriptParseError, parse.Reason)

	missingVar := NewGlobalState().Apply(bulkOf(scriptReq("L", `vars {
	account $dst
}
send [USD 1] (
	source = @world
	destination = $dst
)`, nil, false)))
	require.Equal(t, domain.ErrReasonNumscriptExecutionError, missingVar.Reason)

	unknown := NewGlobalState().Apply(bulkOf(referenceReq("L", "ns", "latest", nil)))
	require.Equal(t, domain.ErrReasonNumscriptNotFound, unknown.Reason)

	both := NewGlobalState().Apply(bulkOf(createReq("L", &servicepb.CreateTransactionPayload{
		Script:          &commonpb.Script{Plain: sendTen},
		ScriptReference: &servicepb.ScriptReference{Name: "ns", Version: "latest"},
	})))
	require.Equal(t, domain.ErrReasonValidation, both.Reason)
}

// A missing metadata read is state-dependent: under the workload's idempotency
// key admission forwards it, and the FSM refuses it as PRELOAD_UNAVAILABLE.
func TestScript_StateDependentResolutionForwarded(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(scriptReq("L", `vars {
	account $dst = meta(@cfg:1, "dst")
}
send [USD 1] (
	source = @world
	destination = $dst
)`, nil, false)))
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonPreloadUnavailable, res.Reason)
}

func TestScript_LibraryVersions(t *testing.T) {
	t.Parallel()

	saved := NewGlobalState().Apply(bulkOf(
		saveReq("L", "ns", "1.2.0", sendTen),
		saveReq("L", "ns", "1.10.0", `send [EUR 1] (
	source = @world
	destination = @b:1
)`),
		saveReq("L", "ns", "1.3.0", sendTen),
	))
	require.True(t, saved.OK)
	require.Equal(t, "1.10.0", saved.State.Ledger("L").NumscriptLatest("ns"))
	require.Equal(t, "ns", saved.Orders[0].SavedNumscript.GetName())
	require.Empty(t, saved.State.Ledger("L").LogIDs(), "a save takes no ledger-local log id")

	dup := saved.State.Apply(bulkOf(saveReq("L", "ns", "1.2.0", sendTen)))
	require.Equal(t, domain.ErrReasonNumscriptVersionAlreadyExists, dup.Reason)

	badVersion := saved.State.Apply(bulkOf(saveReq("L", "ns", "1.2", sendTen)))
	require.Equal(t, domain.ErrReasonNumscriptInvalidVersion, badVersion.Reason)

	badScript := saved.State.Apply(bulkOf(saveReq("L", "ns", "2.0.0", "send (")))
	require.Equal(t, domain.ErrReasonNumscriptParseError, badScript.Reason)

	latest := saved.State.Apply(bulkOf(referenceReq("L", "ns", "latest", nil)))
	require.True(t, latest.OK)
	require.Equal(t, "EUR", latest.Orders[0].Postings[0].GetAsset())

	exact := saved.State.Apply(bulkOf(referenceReq("L", "ns", "1.3.0", nil)))
	require.Equal(t, domain.ErrReasonInsufficientFunds, exact.Reason)

	unknownVersion := saved.State.Apply(bulkOf(referenceReq("L", "ns", "9.9.9", nil)))
	require.Equal(t, domain.ErrReasonNumscriptNotFound, unknownVersion.Reason)

	selector := saved.State.Apply(bulkOf(referenceReq("L", "ns", "1", nil)))
	require.Equal(t, domain.ErrReasonNumscriptInvalidVersion, selector.Reason)

	// A save earlier in the same bulk is visible to a reference after it.
	sameBulk := NewGlobalState().Apply(bulkOf(
		saveReq("L", "other", "1.0.0", `send [USD 1] (
	source = @world
	destination = @b:1
)`),
		referenceReq("L", "other", "latest", nil),
	))
	require.True(t, sameBulk.OK)
	require.Equal(t, "other", sameBulk.State.Ledger("L").txs.Get(0).template)
}

func TestScript_FingerprintDistinguishesLibraries(t *testing.T) {
	t.Parallel()

	a := NewGlobalState().Apply(bulkOf(saveReq("L", "ns", "1.0.0", sendTen)))
	b := NewGlobalState().Apply(bulkOf(saveReq("L", "ns", "1.0.1", sendTen)))
	require.NotEqual(t, hashState(a.State), hashState(b.State))
}

// Admission resolves a reference against every save ahead of it in the batch,
// even one the FSM will refuse: a garbled save shadows the library, so the
// reference resolves to text that does not parse instead of to nothing.
func TestScript_ReferenceSeesRefusedSameBulkSave(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(
		saveReq("L", "ns", "1.0.0", "send ("),
		referenceReq("L", "ns", "latest", nil),
	))
	require.Equal(t, domain.ErrReasonNumscriptParseError, res.Reason)

	exact := NewGlobalState().Apply(bulkOf(
		saveReq("L", "ns", "1.0", sendTen),
		referenceReq("L", "ns", "1.0.0", nil),
	))
	require.Equal(t, domain.ErrReasonNumscriptNotFound, exact.Reason)
}

// Every generated script, seeded the way the driver seeds it, gets a verdict
// from the oracle without panicking — and enough of them commit for the
// generator to be worth running.
func TestScript_GeneratedScriptsAreTotal(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(1))
	accounts := make([]string, 15)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("t-%d:%d", i%3, i)
	}

	outcomes := map[string]int{}
	for range 2000 {
		gen := numscriptgen.GenerateFor(rng, accounts)

		var seeds []*servicepb.Request
		for k, amount := range gen.Balances {
			if amount.Sign() > 0 {
				seeds = append(seeds, oracletest.TxReqL("L", "world", k.Account, k.Asset, amount.Int64()))
			} else if amount.Sign() < 0 {
				seeds = append(seeds, createReq("L", &servicepb.CreateTransactionPayload{
					Postings: []*commonpb.Posting{commonpb.NewPosting(k.Account, "world", k.Asset, new(big.Int).Neg(amount))},
					Force:    true,
				}))
			}
		}

		for k, v := range gen.Metadata {
			seeds = append(seeds, &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
				Ledger: "L",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{AddMetadata: &commonpb.SaveMetadataCommand{
					Target:   &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: k.Account}}},
					Metadata: map[string]*commonpb.MetadataValue{k.Key: commonpb.NewStringValue(v)},
				}}},
			}}})
		}

		seeded := NewGlobalState().Apply(bulkOf(seeds...))
		require.True(t, seeded.OK || len(seeds) == 0, seeded.Reason)

		res := seeded.State.Apply(keyedBulk("k", scriptReq("L", gen.Script, gen.Vars, rng.Intn(8) == 0)))
		reason := res.Reason
		if res.OK {
			reason = "OK"
		}

		outcomes[reason]++
	}

	t.Logf("outcomes: %v", outcomes)
	require.Greater(t, outcomes["OK"], 500)
}
