package wallet

import (
	"github.com/formancehq/go-libs/metadata"
)

type Account interface {
	metadata.Owner
	GetAddress() string
}
