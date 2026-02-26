package numscript

import "math/big"

// FeatureFlags contains all experimental Numscript features that are enabled by default.
var FeatureFlags = map[string]struct{}{
	"experimental-account-interpolation":    {},
	"experimental-asset-colors":             {},
	"experimental-get-amount-function":      {},
	"experimental-get-asset-function":       {},
	"experimental-mid-script-function-call": {},
	"experimental-oneof":                    {},
	"experimental-overdraft-function":       {},
}

// MaxForceBalance is returned for all accounts when force mode is enabled.
// This effectively allows any amount to be sent from any account.
var MaxForceBalance = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
