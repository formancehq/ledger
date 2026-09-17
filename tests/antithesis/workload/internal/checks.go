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
		d := details.With(Details{"asset": entry.GetAsset(), "color": entry.GetColor()})
		if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil || vol.GetBalance() == nil {
			assert.Always(false, "account volume entry has missing required fields", d)
			continue
		}
		input, err := vol.GetInput().ToBigInt()
		if err != nil {
			assert.Always(false, "account volume input is invalid", d.With(Details{"error": err.Error()}))
			continue
		}
		output, err := vol.GetOutput().ToBigInt()
		if err != nil {
			assert.Always(false, "account volume output is invalid", d.With(Details{"error": err.Error()}))
			continue
		}
		balance, err := vol.GetBalance().ToBigInt()
		if err != nil {
			assert.Always(false, "account volume balance is invalid", d.With(Details{"error": err.Error()}))
			continue
		}
		CheckVolume(input, output, balance, d)
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
			d := details.With(Details{"account": account, "asset": entry.GetAsset(), "color": entry.GetColor()})
			if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil {
				assert.Always(false, "post-commit volume entry has missing required fields", d)
				continue
			}
			input, err := vol.GetInput().ToBigInt()
			if err != nil {
				assert.Always(false, "post-commit volume input is invalid", d.With(Details{"error": err.Error()}))
				continue
			}
			output, err := vol.GetOutput().ToBigInt()
			if err != nil {
				assert.Always(false, "post-commit volume output is invalid", d.With(Details{"error": err.Error()}))
				continue
			}
			balance := new(big.Int).Sub(input, output)
			CheckVolume(input, output, balance, d)
		}
	}
}
