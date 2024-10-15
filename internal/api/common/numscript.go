package common

import (
	"fmt"
	"github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"sort"
	"strings"

	"github.com/formancehq/go-libs/v2/metadata"
)

type variable struct {
	name  string
	value string
}

func TxToScriptData(txData ledger.TransactionData, allowUnboundedOverdrafts bool) ledgercontroller.RunScript {
	sb := strings.Builder{}
	monetaryToVars := map[string]variable{}
	accountsToVars := map[string]variable{}
	i := 0
	j := 0
	for _, p := range txData.Postings {
		if _, ok := accountsToVars[p.Source]; !ok {
			if p.Source != ledger.WORLD {
				accountsToVars[p.Source] = variable{
					name:  fmt.Sprintf("va%d", i),
					value: p.Source,
				}
				i++
			}
		}
		if _, ok := accountsToVars[p.Destination]; !ok {
			if p.Destination != ledger.WORLD {
				accountsToVars[p.Destination] = variable{
					name:  fmt.Sprintf("va%d", i),
					value: p.Destination,
				}
				i++
			}
		}
		mon := fmt.Sprintf("[%s %s]", p.Amount.String(), p.Asset)
		if _, ok := monetaryToVars[mon]; !ok {
			monetaryToVars[mon] = variable{
				name:  fmt.Sprintf("vm%d", j),
				value: fmt.Sprintf("%s %s", p.Asset, p.Amount.String()),
			}
			j++
		}
	}

	sb.WriteString("vars {\n")
	accVars := make([]string, 0)
	for _, v := range accountsToVars {
		accVars = append(accVars, v.name)
	}
	sort.Strings(accVars)
	for _, v := range accVars {
		sb.WriteString(fmt.Sprintf("\taccount $%s\n", v))
	}
	monVars := make([]string, 0)
	for _, v := range monetaryToVars {
		monVars = append(monVars, v.name)
	}
	sort.Strings(monVars)
	for _, v := range monVars {
		sb.WriteString(fmt.Sprintf("\tmonetary $%s\n", v))
	}
	sb.WriteString("}\n")

	for _, p := range txData.Postings {
		m := fmt.Sprintf("[%s %s]", p.Amount.String(), p.Asset)
		mon, ok := monetaryToVars[m]
		if !ok {
			panic(fmt.Sprintf("monetary %s not found", m))
		}
		sb.WriteString(fmt.Sprintf("send $%s (\n", mon.name))
		if p.Source == ledger.WORLD {
			sb.WriteString("\tsource = @world\n")
		} else {
			src, ok := accountsToVars[p.Source]
			if !ok {
				panic(fmt.Sprintf("source %s not found", p.Source))
			}
			sb.WriteString(fmt.Sprintf("\tsource = $%s", src.name))
			if allowUnboundedOverdrafts {
				sb.WriteString(" allowing unbounded overdraft")
			}
			sb.WriteString("\n")
		}
		if p.Destination == ledger.WORLD {
			sb.WriteString("\tdestination = @world\n")
		} else {
			dest, ok := accountsToVars[p.Destination]
			if !ok {
				panic(fmt.Sprintf("destination %s not found", p.Destination))
			}
			sb.WriteString(fmt.Sprintf("\tdestination = $%s\n", dest.name))
		}
		sb.WriteString(")\n")
	}

	vars := map[string]string{}
	for _, v := range accountsToVars {
		vars[v.name] = v.value
	}
	for _, v := range monetaryToVars {
		vars[v.name] = v.value
	}

	if txData.Metadata == nil {
		txData.Metadata = metadata.Metadata{}
	}

	return ledgercontroller.RunScript{
		Script: ledgercontroller.Script{
			Plain: sb.String(),
			Vars:  vars,
		},
		Timestamp: txData.Timestamp,
		Metadata:  txData.Metadata,
		Reference: txData.Reference,
	}
}
