package core

import (
	"fmt"

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

func MarkReverts(m metadata.Metadata, txID uint64) metadata.Metadata {
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

func RevertedMetadata(by uint64) metadata.Metadata {
	return ComputeMetadata(RevertedMetadataSpecKey(), fmt.Sprint(by))
}

func RevertMetadata(tx uint64) metadata.Metadata {
	return ComputeMetadata(RevertMetadataSpecKey(), fmt.Sprint(tx))
}
