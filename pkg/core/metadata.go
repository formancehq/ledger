package core

import (
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

const (
	formanceNamespace         = "com.formance.spec/"
	revertKey                 = "state/reverts"
	revertedKey               = "state/reverted"
	MetaTargetTypeAccount     = "ACCOUNT"
	MetaTargetTypeTransaction = "TRANSACTION"
)

func SpecMetadata(name string) string {
	return formanceNamespace + name
}

func MarkReverts(m metadata.Metadata, txID string) metadata.Metadata {
	return m.Merge(RevertMetadata(txID))
}

func RevertedMetadataSpecKey() string {
	return SpecMetadata(revertedKey)
}

func RevertMetadataSpecKey() string {
	return SpecMetadata(revertKey)
}

func ComputeMetadata(key, value string) metadata.Metadata {
	return metadata.Metadata{
		key: value,
	}
}

func RevertedMetadata(by string) metadata.Metadata {
	return ComputeMetadata(RevertedMetadataSpecKey(), by)
}

func RevertMetadata(tx string) metadata.Metadata {
	return ComputeMetadata(RevertMetadataSpecKey(), tx)
}
