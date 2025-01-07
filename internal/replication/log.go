package ingester

import (
	ledger "github.com/formancehq/ledger/internal"
)

type LogWithModule struct {
	ledger.Log
	Ledger string
}

func NewLogWithLedger(ledger string, log ledger.Log) LogWithModule {
	return LogWithModule{
		Log:    log,
		Ledger: ledger,
	}
}
