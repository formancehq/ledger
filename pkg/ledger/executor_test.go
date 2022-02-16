package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/numary/ledger/pkg/core"
	machine "github.com/numary/machine/core"
)

func assertBalance(t *testing.T, l *Ledger, account string, asset string, amount int64) {
	user, err := l.GetAccount(context.Background(), account)
	if err != nil {
		t.Error(err)
		return
	}
	if b := user.Balances[asset]; b != amount {
		t.Fatalf(
			"wrong %v balance for account %v, expected: %d got: %d",
			asset,
			account,
			amount,
			b,
		)
	}
}

func TestNoScript(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{}

		_, err := l.Execute(context.Background(), script)
		assert.IsType(t, &ScriptError{}, err)
		assert.Equal(t, ScriptErrorNoScript, err.(*ScriptError).Code)
	})
}

func TestCompilationError(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "willnotcompile",
		}

		_, err := l.Execute(context.Background(), script)
		assert.IsType(t, &ScriptError{}, err)
		assert.Equal(t, ScriptErrorCompilationFailed, err.(*ScriptError).Code)
	})
}

func TestTransactionInvalidScript(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "this is not a valid script",
		}

		_, err := l.Execute(context.Background(), script)

		if err == nil {
			t.Error(errors.New(
				"script was invalid yet the transaction was committed",
			))
		}
		l.Close(context.Background())
	})
}

func TestTransactionFail(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "fail",
		}

		_, err := l.Execute(context.Background(), script)

		if err == nil {
			t.Error(errors.New(
				"script failed yet the transaction was commited",
			))
		}
		l.Close(context.Background())
	})
}

func TestSend(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())
		script := core.Script{
			Plain: `send [USD/2 99] (
				source=@world
				destination=@user:001
			)`,
		}

		_, err := l.Execute(context.Background(), script)

		if err != nil {
			t.Error(err)
			return
		}

		assertBalance(t, l, "user:001", "USD/2", 99)
	})
}

func TestNoVariables(t *testing.T) {
	with(func(l *Ledger) {
		var script core.Script
		json.Unmarshal(
			[]byte(`{
				"plain": "vars {\naccount $dest\n}\nsend [CAD/2 42] (\n source=@world \n destination=$dest \n)",
				"vars": {}
			}`),
			&script)

		_, err := l.Execute(context.Background(), script)

		if err == nil {
			t.Error(errors.New(
				"variables were not provided but the transaction was committed",
			))
		}
		l.Close(context.Background())
	})
}

func TestVariables(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		var script core.Script
		json.Unmarshal(
			[]byte(`{
				"plain": "vars {\naccount $dest\n}\nsend [CAD/2 42] (\n source=@world \n destination=$dest \n)",
				"vars": {
					"dest": "user:042"
				}
			}`),
			&script)

		_, err := l.Execute(context.Background(), script)

		if err != nil {
			t.Error(err)
			return
		}

		user, err := l.GetAccount(context.Background(), "user:042")

		if err != nil {
			t.Error(err)
			return
		}

		if b := user.Balances["CAD/2"]; b != 42 {
			t.Error(fmt.Sprintf(
				"wrong CAD/2 balance for account user:042, expected: %d got: %d",
				42,
				b,
			))
		}
	})
}

func TestEnoughFunds(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		_, _, err := l.Commit(context.Background(), []core.TransactionData{tx})

		if err != nil {
			t.Error(err)
			return
		}

		var script core.Script
		err = json.Unmarshal(
			[]byte(`{
				"plain": "send [COIN 95] (\n source=@user:001 \n destination=@world \n)"
			}`),
			&script)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = l.Execute(context.Background(), script)

		if err != nil {
			t.Error(err)
			return
		}
	})
}

func TestNotEnoughFunds(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:002",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		_, _, err := l.Commit(context.Background(), []core.TransactionData{tx})

		if err != nil {
			t.Error(err)
			return
		}

		var script core.Script
		json.Unmarshal(
			[]byte(`{
				"plain": "send [COIN 105] (\n source=@user:002 \n destination=@world \n)"
			}`),
			&script)

		_, err = l.Execute(context.Background(), script)

		if err == nil {
			t.Error("error wasn't supposed to be nil")
			return
		}
	})
}

func TestMissingMetadata(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		plain := `
			vars {
				account $sale
				account $seller = meta($sale, "seller")
			}

			send [COIN *] (
				source = $sale
				destination = $seller
			)
		`

		script := core.Script{
			Plain: plain,
			Vars: map[string]json.RawMessage{
				"sale": json.RawMessage(`"sales:042"`),
			},
		}

		_, err := l.Execute(context.Background(), script)

		if err == nil {
			t.Fatalf("expected an error because of missing metadata")
		}
	})
}

func TestMetadata(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "sales:042",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		_, _, err := l.Commit(context.Background(), []core.TransactionData{tx})

		l.SaveMeta(context.Background(), "account", "sales:042", core.Metadata{
			"seller": json.RawMessage(`{
				"type":  "account",
				"value": "users:053"
			}`),
		})

		l.SaveMeta(context.Background(), "account", "users:053", core.Metadata{
			"commission": json.RawMessage(`{
				"type":  "portion",
				"value": "15.5%"
			}`),
		})

		if err != nil {
			t.Error(err)
			return
		}

		plain := `
			vars {
				account $sale
				account $seller = meta($sale, "seller")
				portion $commission = meta($seller, "commission")
			}

			send [COIN *] (
				source = $sale
				destination = {
					remaining to $seller
					$commission to @platform
				}
			)
		`
		if err != nil {
			t.Fatalf("did not expect error: %v", err)
		}

		script := core.Script{
			Plain: plain,
			Vars: map[string]json.RawMessage{
				"sale": json.RawMessage(`"sales:042"`),
			},
		}

		_, err = l.Execute(context.Background(), script)

		if err != nil {
			t.Fatalf("execution error: %v", err)
		}

		assertBalance(t, l, "sales:042", "COIN", 0)

		assertBalance(t, l, "users:053", "COIN", 85)

		assertBalance(t, l, "platform", "COIN", 15)
	})
}

func TestSetTxMeta(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close(context.Background())

		plain := `
			vars {
				account $user
			}

			set_tx_meta("test_meta", [COIN 10])

			send [COIN 10] (
				source = @world
				destination = $user
			)
		`

		script := core.Script{
			Plain: plain,
			Vars: map[string]json.RawMessage{
				"user": json.RawMessage(`"user:042"`),
			},
		}

		_, err := l.Execute(context.Background(), script)

		if err != nil {
			t.Fatalf("execution error: %v", err)
		}

		assertBalance(t, l, "user:042", "COIN", 10)

		tx, err := l.GetLastTransaction(context.Background())

		if err != nil {
			t.Fatalf("could not get last transaction: %v", err)
		}

		value, err := machine.NewValueFromTypedJSON(tx.Metadata["test_meta"])

		if err != nil {
			t.Fatalf("tx metadata was invalid: %v", err)
		}

		if !machine.ValueEquals(*value, machine.Monetary{
			Asset:  "COIN",
			Amount: 10,
		}) {
			t.Fatalf("tx metadata was not the expected value")
		}
	})
}
