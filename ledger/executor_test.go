package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/numary/ledger/core"
)

func TestTransactionInvalidScript(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "this is not a valid script",
		}

		err := l.Execute(script)

		if err == nil {
			t.Error(errors.New(
				"script was invalid yet the transaction was commited",
			))
		}
		l.Close()
	})
}

func TestTransactionFail(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "fail",
		}

		err := l.Execute(script)

		if err == nil {
			t.Error(errors.New(
				"script failed yet the transaction was commited",
			))
		}
		l.Close()
	})
}

func TestSend(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close()
		script := core.Script{
			Plain: `send [USD/2 99] (
				source=@world
				destination=@user:001
			)`,
		}

		err := l.Execute(script)

		if err != nil {
			t.Error(err)
			return
		}

		user, err := l.GetAccount("user:001")

		if err != nil {
			t.Error(err)
			return
		}

		if b := user.Balances["USD/2"]; b != 99 {
			t.Error(fmt.Sprintf(
				"wrong USD/2 balance for account user:001, expected: %d got: %d",
				99,
				b,
			))
		}
	})
}

func TestVariables(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close()

		var script core.Script
		json.Unmarshal(
			[]byte(`{
				"plain": "vars {\naccount $dest\n}\nsend [CAD/2 42] (\n source=@world \n destination=$dest \n)",
				"vars": {
					"dest": "user:042"
				}
			}`),
			&script)

		err := l.Execute(script)

		if err != nil {
			t.Error(err)
			return
		}

		user, err := l.GetAccount("user:042")

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
		defer l.Close()

		tx := core.Transaction{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		err := l.Commit([]core.Transaction{tx})

		if err != nil {
			t.Error(err)
			return
		}

		var script core.Script
		json.Unmarshal(
			[]byte(`{
				"plain": "send [COIN 95] (\n source=@user:001 \n destination=@world \n)"
			}`),
			&script)

		err = l.Execute(script)

		if err != nil {
			t.Error(err)
			return
		}
	})
}

func TestNotEnoughFunds(t *testing.T) {
	with(func(l *Ledger) {
		defer l.Close()

		tx := core.Transaction{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "user:002",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		err := l.Commit([]core.Transaction{tx})

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

		err = l.Execute(script)

		if err == nil {
			t.Error("error wasn't supposed to be nil")
			return
		}
	})
}
