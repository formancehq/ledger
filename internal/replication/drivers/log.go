package drivers

import (
	ledger "github.com/formancehq/ledger/internal"
)

type LogWithLedger struct {
	ledger.Log
	Ledger string `json:"ledger"`
}

func NewLogWithLedger(ledger string, log ledger.Log) LogWithLedger {
	return LogWithLedger{
		Log:    log,
		Ledger: ledger,
	}
}
