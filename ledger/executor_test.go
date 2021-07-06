package ledger

import (
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
	})
}

func TestSend(t *testing.T) {
	with(func(l *Ledger) {
		script := core.Script{
			Plain: "send(monetary=[USD/2 99], source=world, destination=user:001)",
		}

		l.Execute(script)

		user, err := l.GetAccount("user:001")

		if err != nil {
			t.Error(err)
		}

		if b := user.Balances["USD/2"]; b != 99 {
			t.Error(fmt.Sprintf(
				"wrong USD/2 balance for account user:001, expected: %d got: %d",
				99,
				b,
			))
		}

		l.Close()
	})
}
