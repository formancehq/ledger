package core

import (
	"strings"
)

func TxsToScriptsData(txsData ...TransactionData) []ScriptData {
	res := []ScriptData{}
	for _, txData := range txsData {
		sb := strings.Builder{}
		for _, p := range txData.Postings {
			sb.WriteString("send [")
			sb.WriteString(p.Asset)
			sb.WriteString(" ")
			sb.WriteString(p.Amount.String())
			sb.WriteString("] (\n\tsource = @")
			sb.WriteString(p.Source)
			sb.WriteString("\n\tdestination = @")
			sb.WriteString(p.Destination)
			sb.WriteString("\n)\n")
		}
		res = append(res, ScriptData{
			Script: Script{
				Plain: sb.String(),
			},
			Timestamp: txData.Timestamp,
			Reference: txData.Reference,
			Metadata:  txData.Metadata,
		})
	}

	return res
}
