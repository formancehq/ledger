package wallet

import (
	"github.com/formancehq/go-libs/metadata"
	"github.com/google/uuid"
)

type DebitHold struct {
	ID          string            `json:"id"`
	WalletID    string            `json:"walletID"`
	Destination Subject           `json:"destination"`
	Asset       string            `json:"asset"`
	Metadata    metadata.Metadata `json:"metadata"`
	Description string            `json:"description"`
}

func (h DebitHold) LedgerMetadata(chart *Chart) metadata.Metadata {
	return metadata.Metadata{
		MetadataKeyWalletSpecType: HoldWallet,
		MetadataKeyHoldWalletID:   h.WalletID,
		MetadataKeyHoldID:         h.ID,
		MetadataKeyHoldAsset:      h.Asset,
		MetadataKeyHoldVoidDestination: map[string]any{
			"type":  "account",
			"value": chart.GetMainBalanceAccount(h.WalletID),
		},
		MetadataKeyHoldDestination: map[string]any{
			"type":  "account",
			"value": h.Destination,
		},
		MetadataKeyWalletCustomData:      map[string]any(h.Metadata),
		MetadataKeyWalletHoldDescription: h.Description,
		MetadataKeyHoldSubject: map[string]any{
			"type":       h.Destination.Type,
			"identifier": h.Destination.Identifier,
			"balance":    h.Destination.Balance,
		},
	}
}

func NewDebitHold(walletID string, destination Subject, asset, description string, md metadata.Metadata) DebitHold {
	return DebitHold{
		ID:          uuid.NewString(),
		WalletID:    walletID,
		Destination: destination,
		Asset:       asset,
		Metadata:    md,
		Description: description,
	}
}

func DebitHoldFromLedgerAccount(account Account) DebitHold {
	subjectMetadata := account.GetMetadata()[MetadataKeyHoldSubject].(map[string]any)
	subject := Subject{
		Type:       subjectMetadata["type"].(string),
		Identifier: subjectMetadata["identifier"].(string),
		Balance: func() string {
			if balance := subjectMetadata["balance"]; balance != nil {
				return balance.(string)
			}
			return ""
		}(),
	}

	hold := DebitHold{}
	hold.ID = account.GetMetadata()[MetadataKeyHoldID].(string)
	hold.WalletID = account.GetMetadata()[MetadataKeyHoldWalletID].(string)
	hold.Destination = subject
	hold.Asset = account.GetMetadata()[MetadataKeyHoldAsset].(string)
	hold.Metadata = account.GetMetadata()[MetadataKeyWalletCustomData].(map[string]any)
	hold.Description = account.GetMetadata()[MetadataKeyWalletHoldDescription].(string)
	return hold
}

type ExpandedDebitHold struct {
	DebitHold
	OriginalAmount MonetaryInt `json:"originalAmount"`
	Remaining      MonetaryInt `json:"remaining"`
}

func (h ExpandedDebitHold) IsClosed() bool {
	return h.Remaining.Uint64() == 0
}

func ExpandedDebitHoldFromLedgerAccount(account interface {
	Account
	GetVolumes() map[string]map[string]int64
	GetBalances() map[string]int64
},
) ExpandedDebitHold {
	hold := ExpandedDebitHold{
		DebitHold: DebitHoldFromLedgerAccount(account),
	}
	hold.OriginalAmount = *NewMonetaryInt(account.GetVolumes()[hold.Asset]["input"])
	hold.Remaining = *NewMonetaryInt(account.GetBalances()[hold.Asset])
	return hold
}

type ConfirmHold struct {
	HoldID    string `json:"holdID"`
	Amount    MonetaryInt
	Reference string
	Final     bool
}

func (c ConfirmHold) resolveAmount(hold ExpandedDebitHold) (uint64, error) {
	amount := hold.Remaining.Uint64()
	if c.Amount.Uint64() != 0 {
		if c.Amount.Uint64() > amount {
			return 0, ErrInsufficientFundError
		}
		amount = c.Amount.Uint64()
	}
	return amount, nil
}

type VoidHold struct {
	HoldID string `json:"holdID"`
}
