package drivers

import (
	"encoding/json"

	ledger "github.com/formancehq/ledger/internal"
)

type LogWithLedger struct {
	ledger.Log
	Ledger string `json:"ledger"`
}

func (l *LogWithLedger) UnmarshalJSON(data []byte) error {
	if err := l.Log.UnmarshalJSON(data); err != nil {
		return err
	}

	type aux struct {
		Ledger string `json:"ledger"`
	}
	var a aux
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	l.Ledger = a.Ledger

	return nil
}

func NewLogWithLedger(ledger string, log ledger.Log) LogWithLedger {
	return LogWithLedger{
		Log:    log,
		Ledger: ledger,
	}
}
