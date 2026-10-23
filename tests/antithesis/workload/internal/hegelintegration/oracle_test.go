package hegelintegration

import (
	"math/big"
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type cell struct{ account, asset string }
type volume struct{ input, output big.Int }
type oracle map[cell]*volume
type intent struct {
	source, destination, asset string
	amount                     big.Int
}

// Decode the input's unsigned little-endian limbs with math/big, independently
// of Ledger's uint256 arithmetic and response values. Own every amount copy.
func capture(ps []*commonpb.Posting) []intent {
	result := make([]intent, len(ps))
	for i, p := range ps {
		result[i].source, result[i].destination, result[i].asset = p.GetSource(), p.GetDestination(), p.GetAsset()
		a := p.GetAmount()
		for _, limb := range []uint64{a.GetV3(), a.GetV2(), a.GetV1(), a.GetV0()} {
			result[i].amount.Lsh(&result[i].amount, 64)
			result[i].amount.Add(&result[i].amount, new(big.Int).SetUint64(limb))
		}
	}

	return result
}

func hasValue(ps []intent) bool {
	for _, p := range ps {
		if p.amount.Sign() != 0 {
			return true
		}
	}

	return false
}

func (m oracle) add(ps []intent, reverse bool) {
	for _, p := range ps {
		src, dst := p.source, p.destination
		if reverse {
			src, dst = dst, src
		}
		for _, account := range []string{src, dst} {
			key := cell{account, p.asset}
			if m[key] == nil {
				m[key] = new(volume)
			}
		}
		out, in := m[cell{src, p.asset}], m[cell{dst, p.asset}]
		out.output.Add(&out.output, &p.amount)
		in.input.Add(&in.input, &p.amount)
	}
}

func (e *episode) check() {
	keys := make([]cell, 0, len(e.model))
	for key := range e.model {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].account != keys[j].account {
			return keys[i].account < keys[j].account
		}

		return keys[i].asset < keys[j].asset
	})
	totals := make(map[string]*volume)
	balances := make(map[string]*big.Int)
	for _, key := range keys {
		want := e.model[key]
		got, err := e.client.GetAccount(e.ctx, &servicepb.GetAccountRequest{Ledger: e.ledger, Address: key.account})
		if err != nil {
			e.ht.Fatalf("GetAccount %v: %v", key, err)
		}
		v := got.FindVolume(key.asset, "")
		if v == nil {
			e.ht.Fatalf("missing volume %v", key)
		}
		input, err := v.GetInput().ToBigInt()
		if err != nil {
			e.ht.Fatalf("invalid input %v: %v", key, err)
		}
		output, err := v.GetOutput().ToBigInt()
		if err != nil {
			e.ht.Fatalf("invalid output %v: %v", key, err)
		}
		balance, err := v.GetBalance().ToBigInt()
		if err != nil {
			e.ht.Fatalf("invalid balance %v: %v", key, err)
		}
		expectedBalance := new(big.Int).Sub(&want.input, &want.output)
		if input.Cmp(&want.input) != 0 || output.Cmp(&want.output) != 0 || balance.Cmp(expectedBalance) != 0 {
			e.ht.Fatalf("volume mismatch %v: got input=%s output=%s balance=%s; want input=%s output=%s balance=%s", key, input, output, balance, &want.input, &want.output, expectedBalance)
		}
		if totals[key.asset] == nil {
			totals[key.asset] = new(volume)
			balances[key.asset] = new(big.Int)
		}
		total := totals[key.asset]
		total.input.Add(&total.input, input)
		total.output.Add(&total.output, output)
		balances[key.asset].Add(balances[key.asset], balance)
	}
	for asset, total := range totals {
		if total.input.Cmp(&total.output) != 0 {
			e.ht.Fatalf("input/output conservation failed for %s: input=%s output=%s", asset, &total.input, &total.output)
		}
		if balances[asset].Sign() != 0 {
			e.ht.Fatalf("balance conservation failed for %s: %s", asset, balances[asset])
		}
	}
}

func (e *episode) checkFinal() {
	for key, v := range e.model {
		if v.input.Cmp(&v.output) != 0 {
			e.ht.Fatalf("nonzero final balance for %v", key)
		}
	}
}
