package core

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type variable struct {
	name    string
	jsonVal json.RawMessage
}

func TxsToScriptsData(txsData ...TransactionData) []ScriptData {
	res := []ScriptData{}
	for _, txData := range txsData {
		sb := strings.Builder{}
		monetaryToVars := map[string]variable{}
		accountsToVars := map[string]variable{}
		i := 0
		j := 0
		for _, p := range txData.Postings {
			if _, ok := accountsToVars[p.Source]; !ok {
				if p.Source != WORLD {
					accountsToVars[p.Source] = variable{
						name:    fmt.Sprintf("va%d", i),
						jsonVal: json.RawMessage(`"` + p.Source + `"`),
					}
					i++
				}
			}
			if _, ok := accountsToVars[p.Destination]; !ok {
				if p.Destination != WORLD {
					accountsToVars[p.Destination] = variable{
						name:    fmt.Sprintf("va%d", i),
						jsonVal: json.RawMessage(`"` + p.Destination + `"`),
					}
					i++
				}
			}
			mon := fmt.Sprintf("[%s %s]", p.Amount.String(), p.Asset)
			if _, ok := monetaryToVars[mon]; !ok {
				monetaryToVars[mon] = variable{
					name: fmt.Sprintf("vm%d", j),
					jsonVal: json.RawMessage(
						`{"asset":"` + p.Asset + `","amount":` + p.Amount.String() + `}`),
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
			if p.Source == WORLD {
				sb.WriteString("\tsource = @world\n")
			} else {
				src, ok := accountsToVars[p.Source]
				if !ok {
					panic(fmt.Sprintf("source %s not found", p.Source))
				}
				sb.WriteString(fmt.Sprintf("\tsource = $%s\n", src.name))
			}
			if p.Destination == WORLD {
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

		vars := map[string]json.RawMessage{}
		for _, v := range accountsToVars {
			vars[v.name] = v.jsonVal
		}
		for _, v := range monetaryToVars {
			vars[v.name] = v.jsonVal
		}

		res = append(res, ScriptData{
			Script: Script{
				Plain: sb.String(),
				Vars:  vars,
			},
			Timestamp: txData.Timestamp,
			Reference: txData.Reference,
			Metadata:  txData.Metadata,
		})
	}

	return res
}
