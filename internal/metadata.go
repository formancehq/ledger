package ledger

import (
	"math/big"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

const (
	formanceNamespace = "com.formance.spec/"
	revertKey         = "state/reverts"

	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

func SpecMetadata(name string) string {
	return formanceNamespace + name
}

func MarkReverts(m metadata.Metadata, txID *big.Int) metadata.Metadata {
	return m.Merge(RevertMetadata(txID))
}

func RevertMetadataSpecKey() string {
	return SpecMetadata(revertKey)
}

func ComputeMetadata(key, value string) metadata.Metadata {
	return metadata.Metadata{
		key: value,
	}
}

func RevertMetadata(tx *big.Int) metadata.Metadata {
	return ComputeMetadata(RevertMetadataSpecKey(), tx.String())
}
