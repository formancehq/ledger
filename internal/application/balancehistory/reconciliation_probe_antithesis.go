//go:build antithesis

package balancehistory

import "github.com/antithesishq/antithesis-sdk-go/assert"

func reachAntithesisBalanceHistoryReconciled(
	manifestVersion uint64,
	auditWatermark uint64,
	logWatermark uint64,
) {
	assert.Reachable(
		"historical balance: projection reconciled to exact source head",
		map[string]any{
			"manifest_version": manifestVersion,
			"audit_watermark":  auditWatermark,
			"log_watermark":    logWatermark,
		},
	)
}
