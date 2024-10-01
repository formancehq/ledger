package ledger

import (
	"github.com/formancehq/ledger/internal/machine/vm"

	"github.com/formancehq/go-libs/metadata"
)

type RunScript = vm.RunScript
type Script = vm.Script
type ScriptV1 = vm.ScriptV1

type RevertTransaction struct {
	Force           bool
	AtEffectiveDate bool
	TransactionID   int
}

type SaveTransactionMetadata struct {
	TransactionID int
	Metadata      metadata.Metadata
}

type SaveAccountMetadata struct {
	Address  string
	Metadata metadata.Metadata
}

type DeleteTransactionMetadata struct {
	TransactionID int
	Key           string
}

type DeleteAccountMetadata struct {
	Address string
	Key     string
}
