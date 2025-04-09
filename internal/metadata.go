package ledger

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/metadata"
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

func MarkReverts(m metadata.Metadata, txID int) metadata.Metadata {
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

func RevertMetadata(txID int) metadata.Metadata {
	return ComputeMetadata(RevertMetadataSpecKey(), fmt.Sprint(txID))
}
