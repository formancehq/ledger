package wallet

import (
	"github.com/formancehq/go-libs/metadata"
)

const (
	MetadataKeyWalletTransaction     = "wallets/transaction"
	MetadataKeyWalletSpecType        = "wallets/spec/type"
	MetadataKeyWalletID              = "wallets/id"
	MetadataKeyWalletName            = "wallets/name"
	MetadataKeyWalletCustomData      = "wallets/custom_data"
	MetadataKeyHoldWalletID          = "wallets/holds/wallet_id"
	MetadataKeyHoldAsset             = "wallets/holds/asset"
	MetadataKeyHoldSubject           = "wallets/holds/subject"
	MetadataKeyHoldID                = "wallets/holds/id"
	MetadataKeyWalletHoldDescription = "wallets/holds/description"
	MetadataKeyHoldVoidDestination   = "void_destination"
	MetadataKeyHoldDestination       = "destination"
	MetadataKeyBalanceName           = "wallets/balances/name"
	MetadataKeyWalletBalance         = "wallets/balances"
	MetadataKeyCreatedAt             = "wallets/createdAt"

	PrimaryWallet = "wallets.primary"
	HoldWallet    = "wallets.hold"

	TrueValue = "true"
)

func TransactionMetadata(customMetadata metadata.Metadata) metadata.Metadata {
	if customMetadata == nil {
		customMetadata = metadata.Metadata{}
	}
	return metadata.Metadata{
		MetadataKeyWalletTransaction: true,
		MetadataKeyWalletCustomData:  customMetadata,
	}
}

func TransactionBaseMetadataFilter() metadata.Metadata {
	return metadata.Metadata{
		MetadataKeyWalletTransaction: true,
	}
}

func IsPrimary(v metadata.Owner) bool {
	return HasMetadata(v, MetadataKeyWalletSpecType, PrimaryWallet)
}

func IsHold(v metadata.Owner) bool {
	return HasMetadata(v, MetadataKeyWalletSpecType, HoldWallet)
}

func GetMetadata(v metadata.Owner, key string) any {
	return v.GetMetadata()[key]
}

func HasMetadata(v metadata.Owner, key, value string) bool {
	return GetMetadata(v, key) == value
}
