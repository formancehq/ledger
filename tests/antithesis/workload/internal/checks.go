package internal

import (
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// CheckVolume verifies that balance == input - output.
func CheckVolume(input, output, balance *big.Int, details Details) {
	actualBalance := new(big.Int).Sub(input, output)
	assert.Always(balance.Cmp(actualBalance) == 0, "reported balance and volumes should be consistent", details.With(Details{
		"input":         input.String(),
		"output":        output.String(),
		"balance":       balance.String(),
		"actualBalance": actualBalance.String(),
	}))
}

// CheckAccountVolumes verifies volume consistency for every (asset, color)
// bucket on an account.
func CheckAccountVolumes(volumes []*commonpb.AccountVolume, details Details) {
	for _, entry := range volumes {
		vol := entry.GetVolumes()
		input, _ := new(big.Int).SetString(vol.GetInput(), 10)
		output, _ := new(big.Int).SetString(vol.GetOutput(), 10)
		balance, _ := new(big.Int).SetString(vol.GetBalance(), 10)
		if input == nil {
			input = big.NewInt(0)
		}
		if output == nil {
			output = big.NewInt(0)
		}
		if balance == nil {
			balance = big.NewInt(0)
		}
		CheckVolume(input, output, balance, details.With(Details{
			"asset": entry.GetAsset(),
			"color": entry.GetColor(),
		}))
	}
}

// CheckPostCommitVolumes verifies volume consistency for post-commit volumes from a transaction response.
// Each (asset, color) bucket is verified independently.
func CheckPostCommitVolumes(pcv *commonpb.PostCommitVolumes, details Details) {
	if pcv == nil {
		return
	}
	for account, volumesByAssets := range pcv.GetVolumesByAccount() {
		for _, entry := range volumesByAssets.GetVolumes() {
			vol := entry.GetVolumes()
			input, _ := new(big.Int).SetString(vol.GetInput(), 10)
			output, _ := new(big.Int).SetString(vol.GetOutput(), 10)
			if input == nil {
				input = big.NewInt(0)
			}
			if output == nil {
				output = big.NewInt(0)
			}
			balance := new(big.Int).Sub(input, output)
			CheckVolume(input, output, balance, details.With(Details{
				"account": account,
				"asset":   entry.GetAsset(),
				"color":   entry.GetColor(),
			}))
		}
	}
}
