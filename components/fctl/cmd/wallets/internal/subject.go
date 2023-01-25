package internal

import (
	"strings"

	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func ParseSubject(subject string, cmd *cobra.Command, client *formance.APIClient) (*formance.Subject, error) {
	var err error
	switch {
	case strings.HasPrefix(subject, "wallet="):
		walletDefinition := strings.TrimPrefix(subject, "wallet=")
		parts := strings.SplitN(walletDefinition, "/", 2)
		balance := "main"
		if len(parts) > 1 {
			balance = parts[1]
		}

		var walletID string
		switch {
		case strings.HasPrefix(walletDefinition, "id:"):
			walletID = strings.TrimPrefix(parts[0], "id:")
		case strings.HasPrefix(walletDefinition, "name:"):
			walletID, err = DiscoverWalletIDFromName(cmd, client, strings.TrimPrefix(parts[0], "name:"))
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("malformed wallet source definition")
		}
		subject := formance.NewWalletSubject("WALLET", walletID)
		subject.SetBalance(balance)
		return &formance.Subject{
			WalletSubject: subject,
		}, nil
	case strings.HasPrefix(subject, "account="):
		return &formance.Subject{
			LedgerAccountSubject: formance.NewLedgerAccountSubject("ACCOUNT", strings.TrimPrefix(subject, "account=")),
		}, nil
	default:
		return nil, errors.New("malformed source definition")
	}
}
