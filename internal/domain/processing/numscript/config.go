package numscript

import "math/big"

// MaxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account.
var MaxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
